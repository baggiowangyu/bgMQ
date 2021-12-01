package mq

import (
	"github.com/gogf/gf/database/gredis"
	"github.com/gogf/gf/os/glog"
	"github.com/gogf/gf/os/grpool"
	"github.com/gogf/gf/util/gconv"
)

type BgMQRedis struct {
	redisClient *gredis.Redis
	//redisConn *gredis.Conn
	msgRecvCallback MessageRecvCallback
	myTopic         string
	pool            *grpool.Pool
}

type MessageRecvCallback func(message string) error

func (b *BgMQRedis) Initialize(redis_conn_string string) error {
	var err error = nil
	b.redisClient, err = gredis.NewFromStr(redis_conn_string)
	if err != nil {
		return err
	}

	b.pool = grpool.New(1)

	return nil
}

func (b *BgMQRedis) SendMessage(topic string, message string) error {
	_, err := b.redisClient.DoVar("PUBLISH", topic, message)
	return err
}

func (b *BgMQRedis) RecvMessage() {
	conn := b.redisClient.Conn()
	_, err := conn.Do("SUBSCRIBE", b.myTopic)
	if err != nil {
		glog.Error(err)

	}

	for {
		reply, err := conn.Receive()
		if err != nil {
			panic(err)
		}

		// 这里应该返回的是三段数据：第一段："message"，是固定的；第二段：[topic]；第三段：[message]
		results := gconv.Strings(reply)
		err = b.msgRecvCallback(results[2])
	}
}

func (b *BgMQRedis) ListenTopic(topic string, callback MessageRecvCallback) {
	b.myTopic = topic
	b.msgRecvCallback = callback

	// 启动工作线程
	err := b.pool.Add(b.RecvMessage)
	if err != nil {
		glog.Error(err)
	}
}
