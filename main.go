package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/segmentio/kafka-go" // Kafka 消费者库
	"golang.org/x/exp/rand"
)

// 配置文件结构
type Config struct {
	AccessId    string
	AccessKey   string
	Endpoint    string
	ProjectName string
}

// 读取配置文件
func readConfig() (*Config, error) {
	// 这里我们假设使用 ini 格式的配置文件
	conf, err := odps.NewConfigFromIni("./conf/config.ini")
	if err != nil {
		return nil, err
	}
	return &Config{
		AccessId:    conf.AccessId,
		AccessKey:   conf.AccessKey,
		Endpoint:    conf.Endpoint,
		ProjectName: conf.ProjectName,
	}, nil
}

const (
	kafkaTopic      = "your_kafka_topic" // Kafka 主题
	kafkaBroker     = "localhost:9092"   // Kafka Broker 地址
	uploadBatchSize = 200                // 批量上传的大小
)

// Record 代表要上传的记录
type Record struct {
	Data data.Record
}

// UploadManager 管理上传请求的处理
type UploadManager struct {
	recordChan  chan Record
	wg          sync.WaitGroup
	tunnel      *tunnel.Tunnel // 保存 Tunnel 引用
	session     *tunnel.StreamUploadSession
	recordBatch []data.Record // 用于存储记录的批次
	config      *Config       // 保存配置
}

// NewUploadManager 创建新的 UploadManager 实例
func NewUploadManager(tunnel *tunnel.Tunnel, session *tunnel.StreamUploadSession, config *Config) *UploadManager {
	return &UploadManager{
		recordChan:  make(chan Record, 10), // 设置记录通道大小
		tunnel:      tunnel,
		session:     session,
		recordBatch: make([]data.Record, 0, uploadBatchSize), // 初始化记录批次
		config:      config,                                  // 保存配置
	}
}

// Start 启动 UploadManager
func (manager *UploadManager) Start() {
	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		manager.processRecords()
	}()
}

// Stop 停止 UploadManager
func (manager *UploadManager) Stop() {
	close(manager.recordChan) // 关闭记录通道
	manager.wg.Wait()         // 等待所有工作者完成
}

// SendRecord 发送单条记录
func (manager *UploadManager) SendRecord(record data.Record) {
	manager.recordChan <- Record{Data: record}
}

// processRecords 处理单条记录
func (manager *UploadManager) processRecords() {
	for record := range manager.recordChan {
		// 将接收到的记录添加到批次中
		manager.recordBatch = append(manager.recordBatch, record.Data)

		// 检查是否达到批量上传的大小
		if len(manager.recordBatch) >= uploadBatchSize {
			err := manager.uploadBatch()
			if err != nil {
				log.Printf("failed to upload batch: %+v", err)
			}
		}
	}

	// 上传剩余的记录
	if len(manager.recordBatch) > 0 {
		err := manager.uploadBatch()
		if err != nil {
			log.Printf("failed to upload remaining records: %+v", err)
		}
	}
}

// uploadBatch 上传记录的批次
func (manager *UploadManager) uploadBatch() error {
	// 创建新的上传会话
	session, err := createUploadSession(manager.tunnel, manager.config)
	if err != nil {
		return err
	}

	packWriter := session.OpenRecordPackWriter()

	for _, record := range manager.recordBatch {
		err := packWriter.Append(record)
		if err != nil {
			return err
		}
	}

	// 刷新数据
	traceId, recordCount, bytesSend, err := packWriter.Flush()
	if err != nil {
		return err
	}

	fmt.Printf(
		"success to upload batch data with traceId=%s, record count=%d, record bytes=%d\n",
		traceId, recordCount, bytesSend,
	)

	// 清空批次
	manager.recordBatch = manager.recordBatch[:0]
	return nil
}

// createUploadSession 创建新的上传会话
func createUploadSession(tunnelIns *tunnel.Tunnel, config *Config) (*tunnel.StreamUploadSession, error) {
	// 格式化当前时间为 YYYYMMDDHH
	partitionKey := time.Now().Format("2006010215") // 例如：2024112022

	session, err := tunnelIns.CreateStreamUploadSession(
		config.ProjectName, // 直接从配置中获取项目名称
		"mc_test_table",
		tunnel.SessionCfg.WithPartitionKey(fmt.Sprintf("ds='%s'", partitionKey)),
		tunnel.SessionCfg.WithCreatePartition(), // 如果指定分区不存在，则创建新的分区
		tunnel.SessionCfg.WithDefaultDeflateCompressor(),
	)
	return session, err
}

// initializeODPS 初始化 ODPS 设置
func initializeODPS(config *Config) (*tunnel.Tunnel, *tunnel.StreamUploadSession, error) {
	// 初始化 ODPS
	aliAccount := account.NewAliyunAccount(config.AccessId, config.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, config.Endpoint)
	odpsIns.SetDefaultProjectName(config.ProjectName)
	project := odpsIns.DefaultProject()

	fmt.Println("default project: " + project.Name())

	// 获取 Tunnel Endpoint
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		return nil, nil, err
	}
	fmt.Println("tunnel endpoint: " + tunnelEndpoint)

	tunnelIns := tunnel.NewTunnel(odpsIns, tunnelEndpoint)

	// 创建新的上传会话
	session, err := createUploadSession(tunnelIns, config)
	if err != nil {
		return nil, nil, err
	}

	return tunnelIns, session, nil
}

// waitForNextHour 等待到下一个整点
func waitForNextHour() {
	now := time.Now()
	nextHour := now.Truncate(time.Hour).Add(time.Hour)
	time.Sleep(time.Until(nextHour))
}

func main() {
	// 读取配置文件
	config, err := readConfig()
	if err != nil {
		log.Fatalf("Failed to read config: %+v", err)
	}

	// 初始化 ODPS
	tunnelIns, session, err := initializeODPS(config)
	if err != nil {
		log.Fatalf("Failed to initialize ODPS: %+v", err)
	}

	// 创建并启动 UploadManager
	manager := NewUploadManager(tunnelIns, session, config)
	manager.Start()

	// 创建 Kafka 消费者
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   kafkaTopic,
		GroupID: "your_group_id", // 消费者组 ID
	})

	// 启动一个 goroutine 每小时在整点更新分区
	go func() {
		for {
			waitForNextHour() // 等待到下一个整点
			log.Println("Updating upload session...")
			newSession, err := createUploadSession(manager.tunnel, manager.config)
			if err != nil {
				log.Printf("failed to create new upload session: %+v", err)
				continue
			}
			manager.session = newSession // 更新 UploadManager 的会话
		}
	}()

	// 处理 Kafka 消息
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("could not read message %v", err)
			continue
		}

		// 处理接收到的消息，假设消息内容是 JSON 格式
		record, err := makeRecordFromMessage(msg.Value)
		if err != nil {
			log.Printf("could not create record from message: %v", err)
			continue
		}

		// 将记录发送到 UploadManager
		manager.SendRecord(record)
	}

	// 停止 UploadManager
	manager.Stop()
}

func makeRecordFromMessage(msg []byte) (data.Record, error) {
	// 假设消息是 JSON 格式并解析为记录
	// 实际实现中，你需要根据消息格式解析数据
	// 这里我们简单地创建一个随机记录
	rand.Seed(uint64(time.Now().UnixNano()))
	randomInt := rand.Intn(100)

	s := data.String("hello world")
	record := []data.Data{
		s,
		data.BigInt(randomInt),
	}

	return record, nil
}
