package dq

import "time"

// Msg 消息
type Msg struct {
	Topic     string        // 消息的主题
	Key       string        // 消息的Key
	Body      []byte        // 消息的Body
	Partition int           // 分区号
	Delay     time.Duration // 延迟时间（秒）
	ReadyTime time.Time     // 消息准备好执行的时间（now + delay）
}
