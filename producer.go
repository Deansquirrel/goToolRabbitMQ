package goToolRabbitMQ

import (
	"context"
	"errors"
	log "github.com/Deansquirrel/goToolLog"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

type producer struct {
	Tag                string
	ConnStr            string
	ContentType        string
	connection         *amqp.Connection
	channel            *amqp.Channel
	notifyClose        chan *amqp.Error
	notifyConfirm      chan amqp.Confirmation
	isConnected        bool
	reConnectDelay     time.Duration
	reSendDelay        time.Duration
	reSendTimes        uint32
	waitConfirmTimeout time.Duration
	//lastConnErr error
	notifyConnErr chan<- *RabbitMQError
	ctx           context.Context
	cancelFunc    context.CancelFunc

	notify sync.RWMutex
}

func (p *producer) NotifyConnErr(notifyConnErr chan<- *RabbitMQError) {
	p.notify.Lock()
	defer p.notify.Unlock()
	//if p.notifyConnErr != nil {
	//	close(p.notifyConnErr)
	//}
	p.notifyConnErr = notifyConnErr
}

func (p *producer) reConnection() {
	for {
		p.isConnected = false
		var err error
		for {
			err = p.connect()
			if err != nil {
				//p.lastConnErr = err
				log.Debug(err.Error())
				if p.notifyConnErr != nil {
					e := RabbitMQError{
						Tag:   p.Tag,
						Type:  ErrorTypeProducer,
						Error: err,
					}
					p.notifyConnErr <- &e
				}
				time.Sleep(p.reConnectDelay)
			} else {
				//p.lastConnErr = nil
				break
			}
		}
		select {
		case <-p.ctx.Done():
			return
		case <-p.notifyClose:
		}
	}
}

func (p *producer) connect() error {
	conn, err := amqp.Dial(p.ConnStr)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}
	_ = ch.Confirm(false)
	p.changeConnection(conn, ch)
	p.isConnected = true
	return nil
}

func (p *producer) changeConnection(connection *amqp.Connection, channel *amqp.Channel) {
	p.connection = connection
	p.channel = channel
	p.notifyClose = make(chan *amqp.Error)
	p.notifyConfirm = make(chan amqp.Confirmation)
	p.channel.NotifyClose(p.notifyClose)
	p.channel.NotifyPublish(p.notifyConfirm)
}

func (p *producer) Publish(exchange string, key string, body string) error {
	MaxTime := p.reSendTimes
	currTime := uint32(0)
	for {
		err := p.UPublish(exchange, key, body)
		if err != nil {
			currTime++
			if currTime <= MaxTime {
				time.Sleep(p.reSendDelay)
				continue
			} else {
				return err
			}
		}
		break
	}
	ticker := time.NewTicker(p.waitConfirmTimeout)
	select {
	case confirm := <-p.notifyConfirm:
		//fmt.Println(confirm.Ack)
		//fmt.Println(confirm.DeliveryTag)
		if confirm.Ack {
			return nil
		}
	case <-ticker.C:
	}
	return errors.New("wait confirm timeout")
	//if p.lastConnErr != nil {
	//	return p.lastConnErr
	//} else {
	//	return errors.New("wait confirm timeout")
	//}
}

func (p *producer) UPublish(exchange string, key string, body string) error {
	if !p.isConnected {
		return errors.New("connection is not already")
	}
	return p.channel.Publish(
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     p.ContentType, //"text/plain",
			ContentEncoding: "",
			Body:            []byte(body),
			DeliveryMode:    amqp.Transient,
			Priority:        0,
			Timestamp:       time.Now(),
		})
}

func (p *producer) Close() error {
	p.cancelFunc()
	if !p.isConnected {
		return nil
	}
	err := p.channel.Close()
	if err != nil {
		return err
	}
	err = p.connection.Close()
	if err != nil {
		return err
	}
	p.isConnected = false
	return nil
}
