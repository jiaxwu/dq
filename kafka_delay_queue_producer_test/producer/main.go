package main

import (
	"github.com/Shopify/sarama"
	"github.com/jiaxwu/dq"
	"github.com/jiaxwu/dq/kafka_delay_queue_producer_test"
	"log"
	"strconv"
	"time"
)

func main() {
	consumerConfig := sarama.NewConfig()
	consumerGroup, err := sarama.NewConsumerGroup(
		kafka_delay_queue_producer_test.Addrs, kafka_delay_queue_producer_test.DelayGroup, consumerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer consumerGroup.Close()

	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producer, err := sarama.NewSyncProducer(kafka_delay_queue_producer_test.Addrs, producerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	delayQueueProducer := dq.NewKafkaDelayQueueProducer(producer, consumerGroup, time.Second*10,
		kafka_delay_queue_producer_test.DelayTopic, kafka_delay_queue_producer_test.RealTopic)
	for i := 0; i < 10; i++ {
		msg := &sarama.ProducerMessage{
			Topic:     kafka_delay_queue_producer_test.RealTopic,
			Value:     sarama.ByteEncoder("test" + strconv.Itoa(i)),
			Timestamp: time.Now(),
		}
		if _, _, err := delayQueueProducer.SendMessage(msg); err != nil {
			log.Println(err)
		}
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second * 10)
}
