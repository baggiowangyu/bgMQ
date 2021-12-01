package main

import (
	"bgMQ/mq"
	"github.com/gogf/gf/os/glog"
	"time"
)

func main() {
	mq1 := new(mq.BgMQRedis)
	mq2 := new(mq.BgMQRedis)

	err := mq1.Initialize("127.0.0.1:6379,0")
	if err != nil {
		glog.Errorf("mq1.Initialize() failed.", err)
		return
	}

	mq1.ListenTopic("topic_mq1", func(message string) error {
		glog.Infof("mq1 received message : %s", message)

		err := mq1.SendMessage("topic_mq2", message)

		return err
	})

	err = mq2.Initialize("127.0.0.1:6379,0")
	if err != nil {
		glog.Errorf("mq2.Initialize() failed.", err)
		return
	}

	mq2.ListenTopic("topic_mq2", func(message string) error {
		glog.Infof("mq2 received message : %s", message)

		err := mq1.SendMessage("topic_mq1", message)

		return err
	})

	err = mq1.SendMessage("topic_mq2", "This is a message form mq1")
	if err != nil {
		glog.Errorf("mq2.Initialize() failed.", err)
		return
	}

	for {
		time.Sleep(time.Duration(999))
	}
}
