package dq

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestNewSimpleRedisDelayQueue(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	q := NewSimpleRedisDelayQueue(client)
	topic := "test"
	count := 10
	var wg sync.WaitGroup
	wg.Add(count)
	go q.Consume(topic, 5, func(msg *Msg) error {
		fmt.Printf("%+v\n", msg)
		wg.Done()
		return nil
	})
	for i := 0; i < count; i++ {
		q.Push(context.Background(), &Msg{
			Topic: topic,
			Key:   topic + strconv.Itoa(i),
			Body:  []byte(topic + strconv.Itoa(i)),
			Delay: time.Second * time.Duration(count),
		})
		time.Sleep(time.Second)
	}
	wg.Wait()
}
