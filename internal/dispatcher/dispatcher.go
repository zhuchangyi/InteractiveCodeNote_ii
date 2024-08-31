package dispatcher

import (
	"context"
	"log"
	"sync"
	"time"

	"interactcode_note_ii/internal/container"
	"interactcode_note_ii/internal/queue"
)

type Dispatcher struct {
	containerManager *container.ContainerManager
	globalQueue      *queue.RedisQueue
	stopChan         chan struct{}
	smallTimeout     time.Duration
	bigTimeout       time.Duration
	resultChan       chan TaskResult
}

type TaskResult struct {
	TaskID string
	Result string
	Error  error
}

func NewDispatcher(cm *container.ContainerManager, gq *queue.RedisQueue) *Dispatcher {
	return &Dispatcher{
		containerManager: cm,
		globalQueue:      gq,
		stopChan:         make(chan struct{}),
		smallTimeout:     5 * time.Second,
		bigTimeout:       10 * time.Second,
		resultChan:       make(chan TaskResult, 100),
	}
}

func (d *Dispatcher) Start() {
	go d.runSmallContainerDispatcher()
	go d.runBigContainerDispatcher()
	go d.handleResults()
}

func (d *Dispatcher) Stop() {
	close(d.stopChan)
}

func (d *Dispatcher) handleResults() {
	for result := range d.resultChan {
		if result.Error != nil {
			log.Printf("Task %s failed: %v", result.TaskID, result.Error)
			// Implement retry logic here if needed
		} else {
			log.Printf("Task %s completed successfully", result.TaskID)
			// Send result back to client (implement this part)
		}
	}
}

func (d *Dispatcher) runSmallContainerDispatcher() {
	for {
		select {
		case <-d.stopChan:
			return
		default:
			d.dispatchToSmallContainers()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (d *Dispatcher) runBigContainerDispatcher() {
	for {
		select {
		case <-d.stopChan:
			return
		default:
			d.dispatchToBigContainers()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (d *Dispatcher) dispatchToSmallContainers() {
	smallContainers := d.containerManager.GetSmallContainers()

	var wg sync.WaitGroup

	for {
		task, err := d.globalQueue.Dequeue()
		if err != nil || task == nil {
			break // 如果队列为空，退出循环
		}

		// 负载均衡：找到任务队列最短的小容器
		var leastLoadedContainer *container.SmallContainer
		minQueueLength := int(^uint(0) >> 1) // 初始化为最大整数
		for _, sc := range smallContainers {
			if len(sc.TaskQueue) < minQueueLength {
				leastLoadedContainer = sc
				minQueueLength = len(sc.TaskQueue)
			}
		}
		log.Printf("minQueueLength: %d", minQueueLength)

		// 没有合适的容器，任务重新入队
		if leastLoadedContainer == nil {
			d.globalQueue.Enqueue(queue.Task(*task))
			continue
		}

		wg.Add(1)
		go func(sc *container.SmallContainer) {
			defer wg.Done()
			containerTask := container.Task{
				ID:   task.ID,
				Code: task.Code,
			}
			select {
			case sc.TaskQueue <- containerTask:
				log.Printf("Task assigned to small container %s", sc.ID)
				go func() {
					err := sc.ExecuteTask(d.containerManager, containerTask, context.Background())
					if err != nil {
						if err == context.DeadlineExceeded {
							d.transferTaskToBigContainer(containerTask)
						} else {
							log.Printf("Error executing task in small container: %v", err)
						}
					}
				}()
			default:
				log.Printf("Small container %s task queue is full", sc.ID)
				d.globalQueue.Enqueue(queue.Task(*task))
			}
		}(leastLoadedContainer)
	}

	wg.Wait()
}

func (d *Dispatcher) dispatchToBigContainers() {
	bigContainers := d.containerManager.GetBigContainers()

	for _, bigContainer := range bigContainers {
		select {
		case task := <-bigContainer.TaskQueue:
			go d.executeTaskInBigContainer(bigContainer, task)
		default:
			task, err := d.globalQueue.Dequeue()
			if err != nil {
				log.Printf("Error dequeuing task: %v", err)
				continue
			}
			if task == nil {
				return
			}
			containerTask := container.Task{
				ID:   task.ID,
				Code: task.Code,
			}
			go d.executeTaskInBigContainer(bigContainer, containerTask)
		}
	}
}

func (d *Dispatcher) transferTaskToBigContainer(task container.Task) {
	bigContainers := d.containerManager.GetBigContainers()

	// 用于记录是否成功将任务分配到大容器
	taskAssigned := false

	for _, bigContainer := range bigContainers {
		select {
		case bigContainer.TaskQueue <- task:
			log.Printf("Task %s transferred to big container %s", task.ID, bigContainer.ID)
			taskAssigned = true
			return
		default:
			continue
		}
	}

	// 如果没有成功分配任务，则处理未分配情况
	if !taskAssigned {
		log.Printf("No available big container for task %s, enqueueing back to global queue", task.ID)
		task := queue.Task{
			ID:   task.ID,
			Code: task.Code,
		}
		d.globalQueue.Enqueue(task)
	}
}

func (d *Dispatcher) executeTaskInBigContainer(bigContainer *container.BigContainer, task container.Task) {
	ctx, cancel := context.WithTimeout(context.Background(), d.bigTimeout)
	defer cancel()

	err := bigContainer.ExecuteTask(d.containerManager, task, ctx)

	bigContainer.CurrentTask = nil

	if err != nil {
		if err == context.DeadlineExceeded {
			log.Printf("Task %s timed out in big container %s, discarding", task.ID, bigContainer.ID)
		} else {
			log.Printf("Error executing task %s in big container %s: %v", task.ID, bigContainer.ID, err)
		}
	}

	d.processQueuedTasks(bigContainer)
}

func (d *Dispatcher) processQueuedTasks(bigContainer *container.BigContainer) {
	if bigContainer.CurrentTask == nil && len(bigContainer.TaskQueue) > 0 {
		nextTask := <-bigContainer.TaskQueue
		bigContainer.CurrentTask = &nextTask
		go func() {
			d.executeTaskInBigContainer(bigContainer, nextTask)
			d.processQueuedTasks(bigContainer)
		}()
	}
}
