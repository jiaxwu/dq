package kafka_delay_queue_example

var (
	Addrs = []string{"127.0.0.1:9092"}
)

const (
	RealTopic  = "RealTopic"
	RealGroup  = "RealGroup"
	DelayTopic = "DelayTopic"
	DelayGroup = "DelayGroup"
)
