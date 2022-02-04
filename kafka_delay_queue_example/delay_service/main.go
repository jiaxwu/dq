package main

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/jiaxwu/dq/kafka_delay_queue_example"
	"log"
	"sync"
	"time"
)

func main() {
	consumerConfig := sarama.NewConfig()
	consumerGroup, err := sarama.NewConsumerGroup(
		kafka_delay_queue_example.Addrs, kafka_delay_queue_example.DelayGroup, consumerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer consumerGroup.Close()

	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producer, err := sarama.NewSyncProducer(kafka_delay_queue_example.Addrs, producerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	consumer := NewConsumer(producer, time.Second*10)
	go func() {
		for {
			if err := consumerGroup.Consume(context.Background(),
				[]string{kafka_delay_queue_example.DelayTopic}, consumer); err != nil {
				break
			}
		}
		defer wg.Done()
	}()
	wg.Wait()
}

type Consumer struct {
	producer sarama.SyncProducer
	delay    time.Duration
}

func NewConsumer(producer sarama.SyncProducer, delay time.Duration) *Consumer {
	return &Consumer{
		producer: producer,
		delay:    delay,
	}
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// 如果消息已经超时，把消息发送到真实队列
		now := time.Now()
		if now.Sub(message.Timestamp) >= c.delay {
			_, _, err := c.producer.SendMessage(&sarama.ProducerMessage{
				Topic: kafka_delay_queue_example.RealTopic,
				Key:   sarama.ByteEncoder(message.Key),
				Value: sarama.ByteEncoder(message.Value),
			})
			if err == nil {
				session.MarkMessage(message, "")
				continue
			}
			return nil
		}
		// 否则休眠一秒
		time.Sleep(time.Second)
		return nil
	}
	return nil
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}
