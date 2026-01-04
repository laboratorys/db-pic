package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	filepath "path/filepath"
	"sync"
	"time"

	"resty.dev/v3"
)

const (
	DOUBAN_API = `
	[{
		"url":"https://m.douban.com",
		"type":"cdn"
	},{
		"url":"https://m.douban.cmliussss.com",
		"type":"cdn"
	},{
		"url":"https://m.douban.cmliussss.net",
		"type":"cdn"
	},{
		"url":"https://ciao-cors.is-an.org/",
		"type":"proxy"
	}]`
	defaultPageSize    = 25
	defaultDelay       = 500 * time.Millisecond
	requestTimeout     = 5 * time.Second
	maxRetryPerRequest = 3
)

var DOUBAN_IMAGE_API = []string{
	"https://img.doubanio.cmliussss.com",
	"https://img.doubanio.cmliussss.net",
	"https://img1.doubanio.com",
	"https://img3.doubanio.com",
	"https://img9.doubanio.com",
}

func main() {
	client, err := NewDoubanClient()
	var allpics []string
	if err == nil {
		//电视剧
		tvs, _ := client.GetDouBanHot("tv", "tv", "tv", "tv")
		if tvs != nil {
			allpics = append(allpics, tvs...)
			log.Printf("[douban_tv] count: %d", len(tvs))
		}
		//电影
		movies, _ := client.GetDouBanHot("movie", "热门", "全部", "movie")
		if movies != nil {
			allpics = append(allpics, movies...)
			log.Printf("[douban_tv] count: %d", len(movies))
		}
		//综艺
		shows, _ := client.GetDouBanHot("tv", "show", "show", "show")
		if shows != nil {
			allpics = append(allpics, shows...)
			log.Printf("[douban_show] count: %d", len(shows))
		}
		//动漫
		cartoons, _ := client.GetDouBanHot("tv", "tv", "tv_animation", "cartoon")
		if cartoons != nil {
			allpics = append(allpics, cartoons...)
			log.Printf("[douban_cartoons] count: %d", len(cartoons))
		}
		//纪录片
		records, _ := client.GetDouBanHot("tv", "tv", "tv_documentary", "record")
		if records != nil {
			allpics = append(allpics, records...)
			log.Printf("[douban_record] count: %d", len(records))
		}
		log.Printf("all pics: %d", len(allpics))
		successes, errs := DownloadFiles(allpics)

		fmt.Println("\n下载结果:")
		for _, file := range successes {
			fmt.Printf("✅成功: %s\n", file)
		}
		for _, err := range errs {
			fmt.Printf("❌失败: %v\n", err)
		}
	}
}

func downloadFileWorker(url string, wg *sync.WaitGroup, successChan chan<- string, errorChan chan<- error) {
	defer wg.Done()

	imagePath, err := extractImagePath(url)
	if err != nil {
		errorChan <- fmt.Errorf("提取路径失败: %v", err)
		return
	}

	fp := filepath.Join(".", filepath.Clean(imagePath))
	dir := filepath.Dir(fp)
	if err := os.MkdirAll(dir, 0755); err != nil {
		errorChan <- fmt.Errorf("创建目录失败: %v", err)
		return
	}

	client := resty.New().
		SetTimeout(15 * time.Second).      // 延长超时时间
		SetRetryCount(3).                  // 单个域名重试次数
		SetRetryWaitTime(2 * time.Second). // 重试间隔
		SetRetryMaxWaitTime(10 * time.Second)

	client.SetHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")

	var lastErr error
	for retry := 0; retry < 2; retry++ { // 整体重试轮次
		for _, domain := range DOUBAN_IMAGE_API {
			fullUrl := domain + imagePath
			_, err := client.R().SetOutputFileName(fp).Get(fullUrl)
			if err != nil {
				lastErr = fmt.Errorf("下载失败 (域名: %s): %v", domain, err)
				continue
			}

			// 增强型文件校验
			if ok, reason := validateDownloadedFile(fp); !ok {
				lastErr = fmt.Errorf("文件校验失败 (域名: %s): %s", domain, reason)
				os.Remove(fp)
				continue
			}

			successChan <- fp
			return
		}
	}

	errorChan <- fmt.Errorf("所有域名尝试失败，最后错误: %v", lastErr)
}

// 文件完整性校验函数
func validateDownloadedFile(filePath string) (bool, string) {
	// 检查文件是否存在
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return false, "文件不存在"
	}

	// 检查文件大小
	if fileInfo.Size() == 0 {
		return false, "0字节文件"
	}

	// 读取文件头部检查魔数
	file, err := os.Open(filePath)
	if err != nil {
		return false, "无法打开文件"
	}
	defer file.Close()

	buf := make([]byte, 8)
	if _, err := file.Read(buf); err != nil {
		return false, "读取文件头失败"
	}

	// JPEG校验 (FF D8)
	if buf[0] == 0xFF && buf[1] == 0xD8 {
		return true, ""
	}
	// PNG校验 (89 50 4E 47)
	if buf[0] == 0x89 && buf[1] == 0x50 && buf[2] == 0x4E && buf[3] == 0x47 {
		return true, ""
	}
	// WEBP校验 (RIFF)
	if string(buf[0:4]) == "RIFF" && string(buf[8:12]) == "WEBP" {
		return true, ""
	}

	return false, "无效的图像文件头"
}

func DownloadFiles(urls []string) ([]string, []error) {
	var wg sync.WaitGroup
	successChan := make(chan string, len(urls))
	errorChan := make(chan error, len(urls))

	// 启动多个goroutine并发下载
	for _, url := range urls {
		wg.Add(1)
		go downloadFileWorker(url, &wg, successChan, errorChan)
	}

	// 等待所有下载完成
	wg.Wait()
	close(successChan)
	close(errorChan)

	// 收集结果
	var successes []string
	var errors []error

	for success := range successChan {
		successes = append(successes, success)
	}

	for err := range errorChan {
		errors = append(errors, err)
	}

	return successes, errors
}

func extractImagePath(fullURL string) (string, error) {
	u, err := url.Parse(fullURL)
	if err != nil {
		return "", fmt.Errorf("URL 解析失败: %v", err)
	}
	return u.Path, nil
}

type DoubanEndpoint struct {
	URL  string `json:"url"`
	Type string `json:"type"`
}

type Rating struct {
	Count     int     `json:"count"`
	Max       int     `json:"max"`
	StarCount float64 `json:"star_count"`
	Value     float64 `json:"value"`
}

type MovieItem struct {
	Rating Rating `json:"rating"`
	Title  string `json:"title"`
	Pic    struct {
		Large  string `json:"large"`
		Normal string `json:"normal"`
	} `json:"pic"`
	IsNew        bool   `json:"is_new"`
	URI          string `json:"uri"`
	EpisodesInfo string `json:"episodes_info"`
	CardSubtitle string `json:"card_subtitle"`
	Type         string `json:"type"`
	ID           string `json:"id"`
}

type HotData struct {
	Items    []MovieItem `json:"items"`
	Category string      `json:"category"`
	Tags     []struct {
		Category string `json:"category"`
		Selected bool   `json:"selected"`
		Types    []struct {
			Selected bool   `json:"selected"`
			Type     string `json:"type"`
			Title    string `json:"title"`
		} `json:"types"`
		Title string `json:"title"`
	} `json:"tags"`
	RecommendTags []struct {
		Category string `json:"category"`
		Selected bool   `json:"selected"`
		Type     string `json:"type"`
		Title    string `json:"title"`
	} `json:"recommend_tags"`
	Total int    `json:"total"`
	Type  string `json:"type"`
}

type DoubanClient struct {
	client    *resty.Client
	endpoints []DoubanEndpoint
}

func NewDoubanClient() (*DoubanClient, error) {
	var endpoints []DoubanEndpoint
	if err := json.Unmarshal([]byte(DOUBAN_API), &endpoints); err != nil {
		return nil, fmt.Errorf("failed to parse Douban API config: %w", err)
	}

	client := resty.New().
		SetTimeout(requestTimeout).
		SetRetryCount(maxRetryPerRequest).
		SetRetryWaitTime(1*time.Second).
		SetHeader("User-Agent", "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1").
		SetHeader("Referer", "https://m.douban.com/movie/")
	return &DoubanClient{
		client:    client,
		endpoints: endpoints,
	}, nil
}

func (dc *DoubanClient) GetDouBanHot(class, category, t, db string) ([]string, error) {
	var pics []string
	currentPage := 0

	for {
		movies, err := dc.fetchPage(class, category, t, db, currentPage)
		if err != nil {
			return nil, err
		}

		if len(movies) == 0 {
			break // 没有更多数据
		}

		pics = append(pics, movies...)
		currentPage++
		time.Sleep(defaultDelay)
	}

	return pics, nil
}

func (dc *DoubanClient) fetchPage(class, category, t, db string, page int) ([]string, error) {
	var lastErr error

	for _, endpoint := range dc.endpoints {
		apiURL := dc.buildAPIURL(endpoint, class, category, t, page)
		var data HotData

		resp, err := dc.client.R().
			SetResult(&data).
			Get(apiURL)

		if err == nil && resp.StatusCode() == 200 {
			return dc.processItems(data.Items), nil
		}

		lastErr = fmt.Errorf("request failed: %s, status: %d, error: %v", endpoint.URL, resp.StatusCode(), err)
		log.Printf("Request error: %s", lastErr.Error())
	}

	return nil, fmt.Errorf("all endpoints failed, last error: %w", lastErr)
}

func (dc *DoubanClient) buildAPIURL(endpoint DoubanEndpoint, class, category, t string, page int) string {
	start := page * defaultPageSize
	baseParams := fmt.Sprintf("start=%d&limit=%d&category=%s&type=%s", start, defaultPageSize, category, t)

	if endpoint.Type == "proxy" {
		return fmt.Sprintf("%s/https://m.douban.com/rexxar/api/v2/subject/recent_hot/%s?%s",
			endpoint.URL, class, baseParams)
	}

	return fmt.Sprintf("%s/rexxar/api/v2/subject/recent_hot/%s?%s",
		endpoint.URL, class, baseParams)
}

func (dc *DoubanClient) processItems(items []MovieItem) []string {
	var pics []string

	for _, item := range items {
		if item.Type == "playlist" {
			continue
		}

		pics = append(pics, item.Pic.Normal)
	}

	return pics
}
