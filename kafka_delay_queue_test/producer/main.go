package main

import (
	"github.com/Shopify/sarama"
	"github.com/jiaxwu/dq/kafka_delay_queue_test"
	"log"
	"strconv"
	"time"
)

func main() {
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producer, err := sarama.NewSyncProducer(kafka_delay_queue_test.Addrs, producerConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 10; i++ {
		msg := &sarama.ProducerMessage{
			Topic:     kafka_delay_queue_test.DelayTopic,
			Value:     sarama.ByteEncoder("test" + strconv.Itoa(i)),
			Timestamp: time.Now(),
		}
		if _, _, err := producer.SendMessage(msg); err != nil {
			log.Println(err)
		}
		time.Sleep(time.Second)
	}
}
