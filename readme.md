概述
该程序使用 Go 语言实现了一个 Kafka 消费者，它接收来自 Kafka 主题的消息，并将这些消息批量上传到阿里云 ODPS（Open Data Processing Service）。程序结构清晰，模块化程度高，使用了多线程来处理消息和上传数据。

关键组件
配置文件:

使用 Config 结构体来存储从配置文件中读取的配置信息，包括阿里云的访问 ID、访问密钥、ODPS 端点和项目名称。
Kafka 消费者:

使用 kafka-go 库来创建 Kafka 消费者，并从指定的主题读取消息。
ODPS 管理:

使用阿里云提供的 Go SDK 来与 ODPS 进行交互，创建上传会话和上传数据。
UploadManager:

负责管理上传操作，包括接收记录、批量上传和创建上传会话。