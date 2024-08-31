package container

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/go-redis/redis/v8"
)

type ContainerManager struct {
	client            *client.Client
	config            *Config
	smallContainers   []*SmallContainer
	bigContainers     []*BigContainer
	smallContainersMu sync.Mutex
	bigContainersMu   sync.Mutex
	redisClient       *redis.Client
}

type Container struct {
	ID string
	//mu sync.Mutex
}

type SmallContainer struct {
	Container
	TaskQueue chan Task
	stopChan  chan struct{}
}

type BigContainer struct {
	Container
	CurrentTask *Task
	TaskQueue   chan Task
}

type Task struct {
	ID   string
	Code string
	// Add other necessary fields
}

func NewContainerManager(config *Config, redisClient *redis.Client) (*ContainerManager, error) {
	cli, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
		client.WithVersion("1.45"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create docker client: %v", err)
	}

	return &ContainerManager{
		client:          cli,
		config:          config,
		smallContainers: make([]*SmallContainer, 0, config.ContainerPool.SmallContainer.Count),
		bigContainers:   make([]*BigContainer, 0, config.ContainerPool.BigContainer.Count),
		redisClient:     redisClient,
	}, nil
}

func (cm *ContainerManager) warmupContainer(containerID string) error {
	ctx := context.Background()
	warmupCode := `
package main

import "fmt"

func main() {
    fmt.Println("Container warmed up up up /n!")
}
`
	warmupDir := "/tmp/warmup"
	warmupFile := filepath.Join(warmupDir, "warmup.go")

	// 使用 printf 的 %q 格式说明符来正确转义 Go 代码
	cmd := fmt.Sprintf("mkdir -p %s && printf '%s' > %s && go run %s",
		warmupDir,
		warmupCode,
		warmupFile,
		warmupFile)

	execConfig := container.ExecOptions{
		Cmd:          []string{"sh", "-c", cmd},
		AttachStdout: true,
		AttachStderr: true,
	}

	execResp, err := cm.client.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return fmt.Errorf("failed to create exec for warmup: %v", err)
	}

	attachResp, err := cm.client.ContainerExecAttach(ctx, execResp.ID, container.ExecAttachOptions{})
	if err != nil {
		return fmt.Errorf("failed to attach exec for warmup: %v", err)
	}
	defer attachResp.Close()

	// 读取输出
	output, err := readDockerOutput(attachResp.Reader)
	if err != nil {
		return fmt.Errorf("failed to read warmup output: %v", err)
	}

	log.Printf("Warmup output for container %s: %s", containerID, output)

	return nil
}

func (cm *ContainerManager) createAndWarmupBigContainer() (*BigContainer, error) {
	container, err := cm.createBigContainer()
	if err != nil {
		return nil, err
	}

	if err := cm.warmupContainer(container.ID); err != nil {
		log.Printf("Warning: Failed to warm up big container %s: %v", container.ID, err)
		// 即使预热失败，我们也继续使用这个容器，只是记录警告
	}

	log.Printf("Big container %s created and warmed up", container.ID)
	return container, nil
}

func readDockerOutput(reader io.Reader) (string, error) {
	var output strings.Builder
	for {
		header := make([]byte, 8)
		_, err := reader.Read(header)
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}

		// 解析 Docker 输出头部
		payloadSize := binary.BigEndian.Uint32(header[4:])
		payload := make([]byte, payloadSize)
		_, err = reader.Read(payload)
		if err != nil {
			return "", err
		}

		output.Write(payload)
	}
	return output.String(), nil
}

func (cm *ContainerManager) InitializeContainers() error {
	var wg sync.WaitGroup
	errChan := make(chan error, cm.config.ContainerPool.SmallContainer.Count+cm.config.ContainerPool.BigContainer.Count)

	for i := 0; i < cm.config.ContainerPool.SmallContainer.Count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			container, err := cm.createAndWarmupSmallContainer()
			if err != nil {
				errChan <- fmt.Errorf("failed to create and warmup small container: %w", err)
				return
			}
			cm.smallContainersMu.Lock()
			cm.smallContainers = append(cm.smallContainers, container)
			cm.smallContainersMu.Unlock()
		}()
	}

	for i := 0; i < cm.config.ContainerPool.BigContainer.Count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			container, err := cm.createAndWarmupBigContainer()
			if err != nil {
				errChan <- fmt.Errorf("failed to create and warmup big container: %w", err)
				return
			}
			cm.bigContainersMu.Lock()
			cm.bigContainers = append(cm.bigContainers, container)
			cm.bigContainersMu.Unlock()
		}()
	}

	wg.Wait()
	close(errChan)

	var errList []error
	for err := range errChan {
		errList = append(errList, err)
	}

	if len(errList) > 0 {
		return fmt.Errorf("failed to initialize containers: %v", errList)
	}

	log.Println("All containers initialized and warmed up successfully")
	return nil
}

func (cm *ContainerManager) createAndWarmupSmallContainer() (*SmallContainer, error) {
	container, err := cm.createSmallContainer()
	if err != nil {
		return nil, err
	}

	if err := cm.warmupContainer(container.ID); err != nil {
		log.Printf("Warning: Failed to warm up small container %s: %v", container.ID, err)
		// 即使预热失败，我们也继续使用这个容器，只是记录警告
	}

	log.Printf("Small container %s created and warmed up", container.ID)
	return container, nil
}

func (cm *ContainerManager) createSmallContainer() (*SmallContainer, error) {
	log.Printf("Creating small container")
	config := &container.Config{
		Image: cm.config.ContainerPool.SmallContainer.Image,
		Cmd:   []string{"tail", "-f", "/dev/null"},
	}
	hostConfig := &container.HostConfig{
		Resources: container.Resources{
			CPUQuota: cm.config.ContainerPool.SmallContainer.CPUQuota,
			Memory:   cm.config.ContainerPool.SmallContainer.Memory * 1024 * 1024 * 1024,
		},
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: cm.config.ContainerPool.SmallContainer.MountSource, // 主机上的路径
				Target: cm.config.ContainerPool.SmallContainer.MountTarget, // 容器内的挂载路径
			},
		},
	}

	resp, err := cm.client.ContainerCreate(context.Background(), config, hostConfig, nil, nil, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create small container: %w", err)
	}

	if err := cm.client.ContainerStart(context.Background(), resp.ID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("failed to start small container: %w", err)
	}
	//预热container
	if err := cm.warmupContainer(resp.ID); err != nil {
		log.Printf("Warning: Failed to warm up small container %s: %v", resp.ID, err)

	} else {
		log.Printf("Small container %s warmed up", resp.ID)
	}

	log.Printf("Small container %s started", resp.ID)

	return &SmallContainer{
		Container: Container{ID: resp.ID},
		TaskQueue: make(chan Task, cm.config.ContainerPool.SmallContainer.QueueSize),
		stopChan:  make(chan struct{}),
	}, nil
}

func (cm *ContainerManager) createBigContainer() (*BigContainer, error) {
	config := &container.Config{
		Image: cm.config.ContainerPool.BigContainer.Image,
		Cmd:   []string{"tail", "-f", "/dev/null"},
	}
	hostConfig := &container.HostConfig{
		Resources: container.Resources{
			CPUQuota: cm.config.ContainerPool.BigContainer.CPUQuota,
			Memory:   cm.config.ContainerPool.BigContainer.Memory * 1024 * 1024 * 1024,
		},
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: cm.config.ContainerPool.BigContainer.MountSource, // 主机上的路径
				Target: cm.config.ContainerPool.BigContainer.MountTarget, // 容器内的挂载路径
			},
		},
	}

	resp, err := cm.client.ContainerCreate(context.Background(), config, hostConfig, nil, nil, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create big container: %w", err)
	}

	if err := cm.client.ContainerStart(context.Background(), resp.ID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("failed to start big container: %w", err)
	}
	//预热container
	if err := cm.warmupContainer(resp.ID); err != nil {
		log.Printf("Warning: Failed to warm up small container %s: %v", resp.ID, err)

	}

	log.Printf("Big container %s started", resp.ID)

	return &BigContainer{
		Container: Container{ID: resp.ID},
		TaskQueue: make(chan Task, cm.config.ContainerPool.BigContainer.QueueSize),
	}, nil
}

func (cm *ContainerManager) Cleanup() {
	for _, sc := range cm.smallContainers {
		cm.client.ContainerRemove(context.Background(), sc.ID, container.RemoveOptions{Force: true})
	}
	for _, bc := range cm.bigContainers {
		cm.client.ContainerRemove(context.Background(), bc.ID, container.RemoveOptions{Force: true})
	}
}

func (cm *ContainerManager) GetSmallContainers() []*SmallContainer {
	cm.smallContainersMu.Lock()
	defer cm.smallContainersMu.Unlock()
	return cm.smallContainers
}

func (cm *ContainerManager) GetBigContainers() []*BigContainer {
	cm.bigContainersMu.Lock()
	defer cm.bigContainersMu.Unlock()
	return cm.bigContainers
}

func (cm *ContainerManager) executeTaskInContainer(ctx context.Context, containerID string, task Task, codeDir string, timeout string) (string, error) {
	log.Printf("Executing executeTaskInContainer task %s in container %s", task.ID, containerID)

	// 使用绝对路径
	fileName := fmt.Sprintf("main_%d.go", time.Now().UnixNano())
	containerFilePath := filepath.Join("/tmp_code", fileName) // 挂载的目录路径
	//log.Printf("Container file path: %s", containerFilePath)

	// 将代码写入宿主机上的挂载目录
	hostFilePath := filepath.Join(codeDir, fileName)
	if err := os.WriteFile(hostFilePath, []byte(task.Code), 0644); err != nil {
		log.Printf("Failed to write code file to host: %v", err)
		return "", fmt.Errorf("failed to write code file to host: %w", err)
	}

	// 执行代码文件
	execConfig := types.ExecConfig{
		Cmd:          []string{"go", "run", containerFilePath},
		AttachStdout: true,
		AttachStderr: true,
	}
	execResp, err := cm.client.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return "", fmt.Errorf("failed to create exec instance: %w", err)
	}

	attachCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resp, err := cm.client.ContainerExecAttach(attachCtx, execResp.ID, container.ExecAttachOptions{})
	if err != nil {
		return "", fmt.Errorf("error attaching exec: %v", err)
	}
	defer resp.Close()

	var output []byte
	outputChan := make(chan []byte, 1)
	errChan := make(chan error, 1)

	go func() {
		var err error
		output, err = io.ReadAll(resp.Reader)
		if err != nil {
			errChan <- fmt.Errorf("error reading exec output: %v", err)
			return
		}
		outputChan <- output
	}()

	defer func() {
		if err := os.Remove(hostFilePath); err != nil {
			log.Printf("Failed to remove file %s: %v", hostFilePath, err)
		}
	}()

	select {
	case <-ctx.Done():
		log.Printf("Task %s in container %s timed out", task.ID, containerID)
		return "timed out", ctx.Err()
	case err := <-errChan:
		log.Printf("Task %s in container %s failed: %v", task.ID, containerID, err)
		return "", err
	case output := <-outputChan:
		if len(output) == 0 {
			log.Printf("Warning: Empty output for task %s in container %s", task.ID, containerID)
			return "", fmt.Errorf("empty output")
		}
		//cleanedOutput := cleanDockerOutput(output)
		log.Printf(" output for task %s  %s: %s", task.ID, containerID, output)
		if err := cm.redisClient.Set(ctx, task.ID, output, 5*time.Minute).Err(); err != nil {
			log.Printf("Failed to store result in Redis for task %s", err)
		}
		return string(output), nil
	}
}

func (sc *SmallContainer) ExecuteTask(cm *ContainerManager, task Task, ctx context.Context) error {
	codeDir := "./tmp_code"
	timeout := "3s"
	taskCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := cm.executeTaskInContainer(taskCtx, sc.ID, task, codeDir, timeout)
	if err != nil {
		log.Printf("Failed to execute task %s in small container %s: %v", task.ID, sc.ID, err)
		return err
	}
	// 从队列中移除任务，
	select {
	case <-sc.TaskQueue:
	default:
		return nil
	}

	return nil
}

func (bc *BigContainer) ExecuteTask(cm *ContainerManager, task Task, ctx context.Context) error {
	taskCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if bc.CurrentTask != nil {
		return fmt.Errorf("big container %s is currently executing another task", bc.ID)
	}

	// 标记当前正在执行的任务
	bc.CurrentTask = &task
	defer func() {
		bc.CurrentTask = nil
	}()

	codeDir := "./tmp_code"
	timeout := "10s" // 大容器的 timeout
	_, err := cm.executeTaskInContainer(taskCtx, bc.ID, task, codeDir, timeout)
	if err != nil {
		log.Printf("Failed to execute task %s in big container %s: %v", task.ID, bc.ID, err)
		return err
	}
	return nil
}
