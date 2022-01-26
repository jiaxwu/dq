package dq

import (
	"context"
	"github.com/Shopify/sarama"
	"time"
)

type KafkaDelayQueueProducer struct {
	producer   sarama.SyncProducer // 生产者
	delayTopic string              // 延迟服务主题
}

// NewKafkaDelayQueueProducer 延迟队列，包含了生产者和延迟服务
// producer 生产者
// delayServiceConsumerGroup 延迟服务消费者
// delayTime 延迟时间
// delayTopic 延迟服务主题
// realTopic 真实队列主题
func NewKafkaDelayQueueProducer(producer sarama.SyncProducer, delayServiceConsumerGroup sarama.ConsumerGroup,
	delayTime time.Duration, delayTopic, realTopic string) *KafkaDelayQueueProducer {
	// 启动延迟服务
	consumer := NewDelayServiceConsumer(producer, delayTime, realTopic)
	go func() {
		for {
			if err := delayServiceConsumerGroup.Consume(context.Background(),
				[]string{delayTopic}, consumer); err != nil {
				break
			}
		}
	}()
	return &KafkaDelayQueueProducer{
		producer:   producer,
		delayTopic: delayTopic,
	}
}

// SendMessage 发送消息
func (q *KafkaDelayQueueProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	msg.Topic = q.delayTopic
	return q.producer.SendMessage(msg)
}

// DelayServiceConsumer 延迟服务消费者
type DelayServiceConsumer struct {
	producer  sarama.SyncProducer
	delay     time.Duration
	realTopic string
}

func NewDelayServiceConsumer(producer sarama.SyncProducer, delay time.Duration,
	realTopic string) *DelayServiceConsumer {
	return &DelayServiceConsumer{
		producer:  producer,
		delay:     delay,
		realTopic: realTopic,
	}
}

func (c *DelayServiceConsumer) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// 如果消息已经超时，把消息发送到真实队列
		now := time.Now()
		if now.Sub(message.Timestamp) >= c.delay {
			_, _, err := c.producer.SendMessage(&sarama.ProducerMessage{
				Topic: c.realTopic,
				Key:   sarama.ByteEncoder(message.Key),
				Value: sarama.ByteEncoder(message.Value),
			})
			if err == nil {
				session.MarkMessage(message, "")
			}
			continue
		}
		// 否则休眠一秒
		time.Sleep(time.Second)
		return nil
	}
	return nil
}

func (c *DelayServiceConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *DelayServiceConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
