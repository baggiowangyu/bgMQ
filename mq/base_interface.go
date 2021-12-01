package mq

type bgMQInterface interface {
	Initialize(addr string) error
	SetReceiveCallback(func())
	SendData(topic string, data string)
}
