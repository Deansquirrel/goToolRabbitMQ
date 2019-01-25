package goToolRabbitMQ

import (
	"context"
	"errors"
	"github.com/streadway/amqp"
	"time"
)

type consumer struct {
	Tag string
	QueueName string
	f func(string)
	ConnStr string
	connection *amqp.Connection
	channel *amqp.Channel
	notifyClose chan *amqp.Error
	isConnected bool
	isHandled bool
	reConnectDelay time.Duration
	lastConnErr error
	ctx context.Context
	cancelFunc context.CancelFunc
}

func(c *consumer)reConnection(){
	for {
		c.isConnected = false
		var err error
		for{
			err = c.connect()
			if err != nil {
				c.lastConnErr = err
				time.Sleep(c.reConnectDelay)
			} else {
				break
			}
		}
		select {
		case <-c.ctx.Done():
			return
		case <-c.notifyClose:
		}
	}
}

func(c *consumer)connect()error{
	conn,err := amqp.Dial(c.ConnStr)
	if err != nil {
		return err
	}
	ch,err := conn.Channel()
	if err != nil {
		return err
	}
	c.changeConnection(conn,ch)
	b,err := c.handleTest()
	if !b {
		return errors.New("handle test err" + err.Error())
	}
	c.isConnected = true
	c.lastConnErr = nil
	return nil
}

func(c *consumer)changeConnection(connection *amqp.Connection, channel *amqp.Channel){
	c.connection = connection
	c.channel = channel
	c.notifyClose = make(chan *amqp.Error)
	c.channel.NotifyClose(c.notifyClose)
}

func(c *consumer)handleTest()(bool,error){
	err := c.startHandler()
	if err != nil {
		return false,err
	} else {
		return true,nil
	}
}

func(c *consumer)reStartHandler(){
	for{
		c.isHandled = false
		for{
			if !c.isConnected {
				time.Sleep(c.reConnectDelay)
			} else {
				break
			}
		}
		select{
		case <-c.ctx.Done():
			return
		case <-c.notifyClose:
		}
	}
}

func(c *consumer)startHandler()error{
	chanD,err := c.channel.Consume(c.QueueName,c.Tag,true,false,false,false,nil)
	if err != nil {
		return err
	}
	go c.handler(chanD,c.f)
	c.isHandled = true
	return nil
}

func (c *consumer)handler(deliveries <-chan amqp.Delivery,f func(string)){
	for d:= range deliveries {
		f(string(d.Body))
	}
}

func (c *consumer)Close() error {
	c.cancelFunc()
	if !c.isConnected {
		return nil
	}
	err := c.channel.Close()
	if err != nil {
		return err
	}
	err = c.connection.Close()
	if err != nil {
		return err
	}
	c.isConnected = false
	return nil
}