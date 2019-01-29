package goToolRabbitMQ

import (
	"context"
	log "github.com/Deansquirrel/goToolLog"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

type consumer struct {
	Tag            string
	QueueName      string
	f              func(string)
	ConnStr        string
	connection     *amqp.Connection
	channel        *amqp.Channel
	notifyClose    chan *amqp.Error
	isConnected    bool
	isHandled      bool
	reConnectDelay time.Duration
	//lastConnErr error
	//lastHandlerErr error
	notifyConnErr    chan<- *RabbitMQError
	notifyHandlerErr chan<- *RabbitMQError
	ctx              context.Context
	cancelFunc       context.CancelFunc

	notify sync.RWMutex
}

func (c *consumer) NotifyConnErr(notifyConnErr chan<- *RabbitMQError) {
	c.notify.Lock()
	defer c.notify.Unlock()
	//if c.notifyConnErr != nil {
	//	close(c.notifyConnErr)
	//}
	c.notifyConnErr = notifyConnErr
}

func (c *consumer) NotifyHandlerErr(notifyHandlerErr chan<- *RabbitMQError) {
	c.notify.Lock()
	defer c.notify.Unlock()
	//if c.notifyHandlerErr != nil {
	//	close(c.notifyHandlerErr)
	//}
	c.notifyHandlerErr = notifyHandlerErr
}

func (c *consumer) reConnection() {
	for {
		c.isConnected = false
		var err error
		for {
			err = c.connect()
			if err != nil {
				//c.lastConnErr = err
				log.Debug(err.Error())
				if c.notifyConnErr != nil {
					e := RabbitMQError{
						Tag:   c.Tag,
						Type:  ErrorTypeConsumer,
						Error: err,
					}
					c.notifyConnErr <- &e
				}
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

func (c *consumer) connect() error {
	log.Debug("RabbitMQ Consumer connecting")
	conn, err := amqp.Dial(c.ConnStr)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	c.changeConnection(conn, ch)
	c.isConnected = true
	log.Debug("RabbitMQ Consumer connected")
	return nil
}

func (c *consumer) changeConnection(connection *amqp.Connection, channel *amqp.Channel) {
	c.connection = connection
	c.channel = channel
	c.notifyClose = make(chan *amqp.Error)
	c.channel.NotifyClose(c.notifyClose)
}

func (c *consumer) reStartHandler() {
	for {
		c.isHandled = false
		var err error
		for {
			if !c.isConnected {
				time.Sleep(c.reConnectDelay)
			} else {
				err = c.startHandler()
				if err != nil {
					//c.lastHandlerErr = err
					log.Debug(err.Error())
					if c.notifyHandlerErr != nil {
						e := RabbitMQError{
							Tag:   c.Tag,
							Type:  ErrorTypeConsumer,
							Error: err,
						}
						c.notifyHandlerErr <- &e
					}
					time.Sleep(c.reConnectDelay)
				} else {
					//c.lastHandlerErr = nil
					break
				}
			}
		}
		select {
		case <-c.ctx.Done():
			return
		case <-c.notifyClose:
		}
	}
}

func (c *consumer) startHandler() error {
	chanD, err := c.channel.Consume(c.QueueName, c.Tag, true, false, false, false, nil)
	if err != nil {
		return err
	}
	go c.handler(chanD, c.f)
	c.isHandled = true
	return nil
}

func (c *consumer) handler(deliveries <-chan amqp.Delivery, f func(string)) {
	log.Debug("RabbitMQ Consumer Handler Begin")
	defer log.Debug("RabbitMQ Consumer Handler End")
	for d := range deliveries {
		f(string(d.Body))
	}
}

func (c *consumer) Close() error {
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
