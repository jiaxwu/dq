package main

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/jiaxwu/dq/kafka_delay_queue_producer_test"
	"log"
	"sync"
	"time"
)

func main() {
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerConfig.Consumer.Offsets.Retry.Max = 3
	consumerConfig.Consumer.Offsets.AutoCommit.Enable = true
	consumerConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	consumerGroup, err := sarama.NewConsumerGroup(kafka_delay_queue_producer_test.Addrs,
		kafka_delay_queue_producer_test.RealGroup, consumerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer consumerGroup.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	consumer := NewConsumer()
	go func() {
		var err error
		for {
			if err = consumerGroup.Consume(context.Background(),
				[]string{kafka_delay_queue_producer_test.RealTopic}, consumer); err != nil {
				break
			}
		}
		defer wg.Done()
	}()
	wg.Wait()
}

type Consumer struct{}

func NewConsumer() *Consumer {
	return &Consumer{}
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		fmt.Println("收到消息：", message.Value, message.Timestamp)
		session.MarkMessage(message, "")
	}
	return nil
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
