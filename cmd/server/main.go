package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"interactcode_note_ii/internal/container"
	"interactcode_note_ii/internal/dispatcher"
	"interactcode_note_ii/internal/queue"

	"github.com/go-redis/redis/v8"
)

const (
	persistentCodeDir = "/interactcode_note_ii/saved_code"
	maxVersions       = 3
)

type CodeVersion struct {
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

type Response struct {
	Output string `json:"output"`
}

var allowedIPs = []string{
	"4.216.100.169",
	"2603:1040:400::257",
}
var resultChannels sync.Map

type GetCodeRequest struct {
	NoteID string `json:"noteId"`
}

type Request struct {
	Code string `json:"code"`
}

type GetCodeResponse struct {
	Code     string        `json:"code"`
	Success  bool          `json:"success"`
	Message  string        `json:"message"`
	Versions []CodeVersion `json:"versions"`
}

type SaveCodeRequest struct {
	Code   string `json:"code"`
	NoteID string `json:"noteId"`
}

type SaveCodeResponse struct {
	Success   bool      `json:"success"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

func enableCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	}
}

func generateTaskID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		log.Fatalf("Failed to generate task ID: %v", err)
	}
	return fmt.Sprintf("%x", b)
}

func cleanDockerOutput(rawOutput []byte) string {
	if len(rawOutput) == 0 {
		log.Println("Warning: Empty raw output")
		return ""
	}

	var cleanedOutput []byte
	for i := 0; i < len(rawOutput); {
		if i+8 > len(rawOutput) {
			log.Printf("Warning: Unexpected end of output at index %d\n", i)
			break
		}

		frameType := rawOutput[i]
		frameSize := int(binary.BigEndian.Uint32(rawOutput[i+4 : i+8]))

		if frameSize == 0 {
			i += 8
			continue
		}

		if i+8+frameSize > len(rawOutput) {
			log.Printf("Warning: Frame size %d exceeds remaining data at index %d\n", frameSize, i)
			break
		}

		if frameType == 1 || frameType == 2 {
			cleanedOutput = append(cleanedOutput, rawOutput[i+8:i+8+frameSize]...)
		}

		i += 8 + frameSize
	}

	return string(cleanedOutput)
}

func handleRunCode(w http.ResponseWriter, r *http.Request, rq *queue.RedisQueue) {
	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	taskID := generateTaskID()
	resultChan := make(chan string)
	resultChannels.Store(taskID, resultChan)

	// 将任务加入队列
	task := queue.Task{
		ID:   taskID,
		Code: req.Code,
	}
	if err := rq.Enqueue(task); err != nil {
		log.Printf("Failed to enqueue task: %v", err)
		http.Error(w, "Unable to process request", http.StatusInternalServerError)
		return
	} else {
		log.Printf("Successfully enqueued task: %v", task.ID)
	}

	// 启动一个 goroutine 来从 Redis 获取结果
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				result, err := rq.GetResultFromRedis(taskID)
				if err != nil {
					log.Printf("Failed to get result from Redis: %v", err)
					continue
				}
				if result != "" {
					resultChan <- result
					return
				}
			case <-resultChan:
				// 如果 resultChan 被关闭，退出循环
				return
			}
		}
	}()

	// 等待结果，设置超时
	select {
	case result := <-resultChan:
		cleanedOutput := cleanDockerOutput([]byte(result))
		response := Response{Output: cleanedOutput}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	case <-time.After(20 * time.Second):
		http.Error(w, "Request timeout", http.StatusRequestTimeout)
	}

	// 清理
	resultChannels.Delete(taskID)
}

func getClientIP(r *http.Request) string {
	ip := r.Header.Get("X-Forwarded-For")
	if ip == "" {
		ip, _, _ = net.SplitHostPort(r.RemoteAddr)
	}
	return ip
}

func isAllowedIP(ip string) bool {
	for _, allowedIP := range allowedIPs {
		if strings.Contains(ip, allowedIP) {
			return true
		}
	}
	return false
}

func handleSaveCode(w http.ResponseWriter, r *http.Request) {
	clientIP := getClientIP(r)
	if !isAllowedIP(clientIP) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	var req SaveCodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// 创建用于保存代码的目录
	noteDir := filepath.Join(persistentCodeDir, req.NoteID)
	if err := os.MkdirAll(noteDir, 0755); err != nil {
		log.Printf("Failed to create note directory: %v\n", err)
		http.Error(w, "Failed to save code", http.StatusInternalServerError)
		return
	}

	// 生成文件名，包含当前时间戳
	timestamp := time.Now()
	fileName := filepath.Join(noteDir, fmt.Sprintf("%d.go", timestamp.UnixNano()))
	if err := os.WriteFile(fileName, []byte(req.Code), 0644); err != nil {
		log.Printf("Failed to save code: %v\n", err)
		http.Error(w, "Failed to save code", http.StatusInternalServerError)
		return
	}

	// 保留最近的三个
	cleanupOldVersions(noteDir)

	// 构建响应信息
	response := SaveCodeResponse{
		Success:   true,
		Message:   "Code saved successfully",
		Timestamp: timestamp,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func cleanupOldVersions(noteDir string) {
	versions, err := getCodeVersions(noteDir)
	if err != nil {
		log.Printf("Failed to get code versions for cleanup: %v\n", err)
		return
	}

	if len(versions) <= maxVersions {
		return
	}

	for _, version := range versions[maxVersions:] {
		fileName := filepath.Join(noteDir, fmt.Sprintf("%d.go", version.Timestamp.UnixNano()))
		if err := os.Remove(fileName); err != nil {
			log.Printf("Failed to remove old version: %v\n", err)
		}
	}
}

func parseTimestamp(fileName string) (time.Time, error) {
	base := filepath.Base(fileName)
	ext := filepath.Ext(base)
	nameWithoutExt := base[:len(base)-len(ext)]
	nanoSeconds, err := strconv.ParseInt(nameWithoutExt, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, nanoSeconds), nil
}

func getCodeVersions(noteDir string) ([]CodeVersion, error) {
	files, err := os.ReadDir(noteDir)
	if err != nil {
		return nil, err
	}

	versions := make([]CodeVersion, 0, len(files))
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".go" {
			content, err := os.ReadFile(filepath.Join(noteDir, file.Name()))
			if err != nil {
				return nil, err
			}
			timestamp, err := parseTimestamp(file.Name())
			if err != nil {
				return nil, err
			}
			versions = append(versions, CodeVersion{
				Content:   string(content),
				Timestamp: timestamp,
			})
		}
	}

	sort.Slice(versions, func(i, j int) bool {
		return versions[i].Timestamp.After(versions[j].Timestamp)
	})

	return versions, nil
}

func handleGetCode(w http.ResponseWriter, r *http.Request) {
	var req GetCodeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	noteDir := filepath.Join(persistentCodeDir, req.NoteID)
	versions, err := getCodeVersions(noteDir)
	if err != nil {
		log.Printf("Failed to get code versions: %v\n", err)
		http.Error(w, "Failed to get code", http.StatusInternalServerError)
		return
	}

	var latestCode string
	if len(versions) > 0 {
		latestCode = versions[0].Content
	}

	response := GetCodeResponse{
		Code:     latestCode,
		Success:  true,
		Message:  "Code retrieved successfully",
		Versions: versions,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	// 加载配置
	config, err := container.LoadConfig("./config/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     config.Redis.Address,
		Password: config.Redis.Password,
		DB:       config.Redis.DB,
	})
	_, err = redisClient.Ping(context.Background()).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	//new个容器管理器
	cm, err := container.NewContainerManager(config, redisClient)
	if err != nil {
		log.Fatalf("Failed to create container manager: %v", err)
	}
	//初始化容器
	if err := cm.InitializeContainers(); err != nil {
		log.Fatalf("Failed to initialize containers: %v", err)
	}
	// 创建 Redis 队列
	rq := queue.NewRedisQueue(redisClient, "tasks")

	// 创建调度器
	d := dispatcher.NewDispatcher(cm, rq)
	// 启动调度器
	d.Start()

	http.HandleFunc("/run", enableCORS(func(w http.ResponseWriter, r *http.Request) {
		handleRunCode(w, r, rq)
	}))
	http.HandleFunc("/saveCode", enableCORS(handleSaveCode))
	http.HandleFunc("/getCode", enableCORS(handleGetCode))

	go func() {
		certFile := "/etc/letsencrypt/live/orjp.leatherpigeon.shop/fullchain.pem"
		keyFile := "/etc/letsencrypt/live/orjp.leatherpigeon.shop/privkey.pem"
		log.Println("Server is running on https://0.0.0.0:8443")
		if err := http.ListenAndServeTLS("0.0.0.0:8443", certFile, keyFile, nil); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// 捕获中断信号
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// 等待中断信号
	<-signalChan
	log.Println("Received interrupt signal, shutting down...")

	// 停止调度器
	d.Stop()

	// 关闭容器管理器

	// 关闭 Redis 连接
	if err := rq.Close(); err != nil {
		log.Printf("Error closing Redis connection: %v", err)
	}

	log.Println("Shutdown complete")
	defer cm.Cleanup()
}
