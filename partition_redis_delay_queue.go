package dq

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"strconv"
	"time"
)

type PartitionRedisDelayQueue struct {
	client     *redis.Client // Redis客户端
	pushScript *redis.Script // Push脚本
	delScript  *redis.Script // Del脚本
}

func NewPartitionRedisDelayQueue(client *redis.Client) *PartitionRedisDelayQueue {
	return &PartitionRedisDelayQueue{
		client:     client,
		pushScript: redis.NewScript(delayQueuePushRedisScript),
		delScript:  redis.NewScript(delayQueueDelRedisScript),
	}
}

func (q *PartitionRedisDelayQueue) Push(ctx context.Context, msg *Msg) error {
	// 如果设置了ReadyTime，就使用RedisTime
	var readyTime int64
	if !msg.ReadyTime.IsZero() {
		readyTime = msg.ReadyTime.Unix()
	} else {
		// 否则使用Delay
		readyTime = time.Now().Add(msg.Delay).Unix()
	}
	success, err := q.pushScript.Run(ctx, q.client,
		[]string{q.topicZSet(msg.Topic, msg.Partition), q.topicHash(msg.Topic, msg.Partition)},
		msg.Key, msg.Body, readyTime).Bool()
	if err != nil {
		return err
	}
	if !success {
		return ErrDuplicateMessage
	}
	return nil
}

func (q *PartitionRedisDelayQueue) Consume(topic string, batchSize, partition int, fn func(msg *Msg) error) {
	for {
		// 批量获取已经准备好执行的消息
		now := time.Now().Unix()
		zs, err := q.client.ZRangeByScoreWithScores(context.Background(), q.topicZSet(topic, partition),
			&redis.ZRangeBy{
				Min:   "-inf",
				Max:   strconv.Itoa(int(now)),
				Count: int64(batchSize),
			}).Result()
		// 如果获取出错或者获取不到消息，则休眠一秒
		if err != nil || len(zs) == 0 {
			time.Sleep(time.Second)
			continue
		}
		// 遍历每个消息
		for _, z := range zs {
			key := z.Member.(string)
			// 获取消息的body
			body, err := q.client.HGet(context.Background(), q.topicHash(topic, partition), key).Bytes()
			if err != nil {
				continue
			}

			// 处理消息
			err = fn(&Msg{
				Topic:     topic,
				Key:       key,
				Body:      body,
				ReadyTime: time.Unix(int64(z.Score), 0),
			})
			if err != nil {
				continue
			}

			// 如果消息处理成功，删除消息
			q.delScript.Run(context.Background(), q.client,
				[]string{q.topicZSet(topic, partition), q.topicHash(topic, partition)}, key)
		}
	}
}

func (q *PartitionRedisDelayQueue) topicZSet(topic string, partition int) string {
	return fmt.Sprintf("%s:%d:z", topic, partition)
}

func (q *PartitionRedisDelayQueue) topicHash(topic string, partition int) string {
	return fmt.Sprintf("%s:%d:h", topic, partition)
}
