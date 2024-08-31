package queue

import (
	"context"
	"encoding/json"
	"log"

	"github.com/go-redis/redis/v8"
)

type Task struct {
	ID   string `json:"id"`
	Code string `json:"code"`
}

type RedisQueue struct {
	client *redis.Client
	key    string
}

func NewRedisQueue(client *redis.Client, key string) *RedisQueue {
	return &RedisQueue{
		client: client,
		key:    key,
	}
}

func (rq *RedisQueue) Enqueue(task Task) error {
	taskJSON, err := json.Marshal(task)
	if err != nil {
		log.Printf("Failed to marshal task: %v", err)
		return err
	}

	err = rq.client.RPush(context.Background(), rq.key, taskJSON).Err()
	if err != nil {
		log.Printf("Failed to enqueue task: %v", err)
		return err
	}

	return nil
}

func (rq *RedisQueue) Dequeue() (*Task, error) {
	result, err := rq.client.LPop(context.Background(), rq.key).Result()
	if err == redis.Nil {
		return nil, nil // 队列为空
	} else if err != nil {
		log.Printf("Failed to dequeue task: %v", err)
		return nil, err
	}

	var task Task
	err = json.Unmarshal([]byte(result), &task)
	if err != nil {
		log.Printf("Failed to unmarshal task: %v", err)
		return nil, err
	}

	return &task, nil
}

func (rq *RedisQueue) Close() error {
	err := rq.client.Close()
	if err != nil {
		log.Printf("Failed to close Redis client: %v", err)
	}
	return err
}

func (rq *RedisQueue) GetResultFromRedis(taskID string) (string, error) {
	// 从 Redis 获取结果
	result, err := rq.client.Get(context.Background(), taskID).Result()
	if err == redis.Nil {
		// 结果不存在
		return "", nil
	} else if err != nil {
		// 出现错误
		log.Printf("Error retrieving result from Redis: %v", err)
		return "", err
	}
	// 返回结果
	return result, nil
}
