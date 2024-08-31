package container

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
)

//yaml到容器池的配置和redis配置

type ContainerPoolConfig struct {
	SmallContainer ContainerConfig `yaml:"small_containers"`
	BigContainer   ContainerConfig `yaml:"big_containers"`
}

type ContainerConfig struct {
	Count       int    `yaml:"count"`
	Image       string `yaml:"image"`
	CPUQuota    int64  `yaml:"cpu_quota"`
	Memory      int64  `yaml:"memory"`
	MountSource string `yaml:"mount_source"`
	MountTarget string `yaml:"mount_target"`
	QueueSize   int    `yaml:"queue_size"`
}

type RedisConfig struct {
	Address  string `yaml:"address"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}
type Config struct {
	ContainerPool ContainerPoolConfig `yaml:"container_pool"`
	Redis         RedisConfig         `yaml:"redis"`
}

func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %v", err)
	}
	defer file.Close()

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var config Config
	err = yaml.Unmarshal(content, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal config file: %v", err)
	}
	return &config, nil
}

// func parseMemory(memoryStr string) (int64, error) {
// 	memoryStr = strings.TrimSpace(memoryStr)
// 	unit := memoryStr[len(memoryStr)-2:]
// 	valueStr := memoryStr[:len(memoryStr)-2]

// 	value, err := strconv.ParseFloat(valueStr, 64)
// 	if err != nil {
// 		return 0, fmt.Errorf("invalid memory size: %s", memoryStr)
// 	}

// 	switch strings.ToUpper(unit) {
// 	case "KB":
// 		return int64(value * 1024), nil
// 	case "MB":
// 		return int64(value * 1024 * 1024), nil
// 	case "GB":
// 		return int64(value * 1024 * 1024 * 1024), nil
// 	case "TB":
// 		return int64(value * 1024 * 1024 * 1024 * 1024), nil
// 	case "B":
// 		return int64(value), nil
// 	default:
// 		return 0, fmt.Errorf("unknown memory unit: %s", unit)
// 	}
// }
