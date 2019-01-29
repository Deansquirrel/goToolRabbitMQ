package goToolRabbitMQ

import (
	"context"
	"errors"
	"fmt"
	log "github.com/Deansquirrel/goToolLog"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

const (
	ErrorTypeConsumer = "Consumer"
	ErrorTypeProducer = "Producer"
)

const (
	connStrFormat = "amqp://%s:%s@%s:%d/%s"

	ReConnectDelay     = time.Second * 5
	ReSendDelay        = time.Microsecond * 500
	ReSendTimes        = uint32(3)
	WaitConfirmTimeout = time.Second * 5
	ConnectTimeout     = time.Second * 30
)

const (
	ExchangeDirect  = amqp.ExchangeDirect
	ExchangeFanout  = amqp.ExchangeFanout
	ExchangeHeaders = amqp.ExchangeHeaders
	ExchangeTopic   = amqp.ExchangeTopic
)

type RabbitMQ struct {
	connection *amqp.Connection

	config *RabbitMQConfig

	ReConnectDelay     time.Duration
	ReSendDelay        time.Duration
	ReSendTimes        uint32
	WaitConfirmTimeout time.Duration
	ConnectTimeout     time.Duration

	notifyErr chan *RabbitMQError

	pList map[string]*producer
	cList map[string]*consumer

	notify sync.RWMutex
}

type RabbitMQConfig struct {
	Server      string
	Port        int
	VirtualHost string
	User        string
	Password    string
}

type RabbitMQError struct {
	Type  string
	Tag   string
	Error error
}

func NewRabbitMQ(config *RabbitMQConfig) (*RabbitMQ, error) {
	r := &RabbitMQ{
		config: config,

		ReConnectDelay:     ReConnectDelay,
		ReSendDelay:        ReSendDelay,
		ReSendTimes:        ReSendTimes,
		WaitConfirmTimeout: WaitConfirmTimeout,
		ConnectTimeout:     ConnectTimeout,

		pList: make(map[string]*producer),
		cList: make(map[string]*consumer),
	}
	b, err := r.connectTest()
	if !b {
		return nil, err
	}
	conn, err := r.getConn()
	if err != nil {
		return nil, err
	}
	r.connection = conn
	return r, nil
}

func (r *RabbitMQ) NotifyErr(notifyErr chan *RabbitMQError) {
	r.notify.Lock()
	defer r.notify.Unlock()
	if r.notifyErr != nil {
		close(r.notifyErr)
	}
	if notifyErr != nil {
		r.notifyErr = notifyErr
	}
}

func (r *RabbitMQ) getConn() (*amqp.Connection, error) {
	conn, err := amqp.Dial(r.getConnStr())
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (r *RabbitMQ) getConnStr() string {
	return fmt.Sprintf(connStrFormat, r.config.User, r.config.Password, r.config.Server, r.config.Port, r.config.VirtualHost)
}

func (r *RabbitMQ) getChannel() (*amqp.Channel, error) {
	ch, err := r.connection.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

func (r *RabbitMQ) ExchangeDeclareSimple(exchange, exchangeType string) error {
	return r.ExchangeDeclare(exchange, exchangeType, true, false, false, false)
}

func (r *RabbitMQ) ExchangeDeclare(exchange, exchangeType string, durable, autoDelete, internal, noWait bool) error {
	ch, err := r.getChannel()
	if err != nil {
		return err
	}
	defer func() {
		_ = ch.Close()
	}()
	err = ch.ExchangeDeclare(exchange, exchangeType, durable, autoDelete, internal, noWait, nil)
	return err
}

func (r *RabbitMQ) ExchangeDelete(exchange string, ifUnused, noWait bool) error {
	ch, err := r.getChannel()
	if err != nil {
		return err
	}
	defer func() {
		_ = ch.Close()
	}()
	return ch.ExchangeDelete(exchange, false, true)
}

func (r *RabbitMQ) QueueDeclareSimple(queue string) error {
	return r.QueueDeclare(queue, true, false, false, false)
}

func (r *RabbitMQ) QueueDeclare(queue string, durable, autoDelete, exclusive, noWait bool) error {
	ch, err := r.getChannel()
	if err != nil {
		return err
	}
	defer func() {
		_ = ch.Close()
	}()
	_, err = ch.QueueDeclare(queue, durable, autoDelete, exclusive, noWait, nil)
	return err
}

func (r *RabbitMQ) QueueDelete(queue string, ifUnused, ifEmpty, noWait bool) error {
	ch, err := r.getChannel()
	if err != nil {
		return err
	}
	defer func() {
		_ = ch.Close()
	}()
	_, err = ch.QueueDelete(queue, ifUnused, ifEmpty, noWait)
	return err
}

func (r *RabbitMQ) QueueBind(queueName, key, exchange string, noWait bool) error {
	ch, err := r.getChannel()
	if err != nil {
		return err
	}
	defer func() {
		_ = ch.Close()
	}()
	err = ch.QueueBind(queueName, key, exchange, noWait, nil)
	return err
}

func (r *RabbitMQ) QueueUnbind(queueName, key, exchange string) error {
	ch, err := r.getChannel()
	if err != nil {
		return err
	}
	defer func() {
		_ = ch.Close()
	}()
	err = ch.QueueUnbind(queueName, key, exchange, nil)
	return err
}

func (r *RabbitMQ) connectTest() (bool, error) {
	conn, err := amqp.Dial(r.getConnStr())
	if err != nil {
		return false, err
	}
	defer func() {
		_ = conn.Close()
	}()
	ch, err := conn.Channel()
	if err != nil {
		return false, err
	}
	defer func() {
		_ = ch.Close()
	}()
	return true, nil
}

func (r *RabbitMQ) AddProducer(tag string) error {
	if _, ok := r.pList[tag]; ok {
		return errors.New("tag is already exists[" + tag + "]")
	}

	b, err := r.connectTest()
	if !b {
		errMsg := "test connect err:"
		if err != nil {
			return errors.New(errMsg + err.Error())
		} else {
			return errors.New(errMsg + "unknown error")
		}
	}

	p := &producer{
		Tag:                tag,
		ConnStr:            r.getConnStr(),
		ContentType:        "text/plain",
		reConnectDelay:     r.ReConnectDelay,
		reSendDelay:        r.ReSendDelay,
		waitConfirmTimeout: r.WaitConfirmTimeout,
	}
	p.ctx, p.cancelFunc = context.WithCancel(context.Background())

	errCh := make(chan *RabbitMQError)
	p.NotifyConnErr(errCh)
	go p.reConnection()

	timeoutCh := make(chan struct{})
	timer := time.AfterFunc(r.ConnectTimeout, func() {
		timeoutCh <- struct{}{}
	})
connectCheck:
	for {
		select {
		case err := <-errCh:
			log.Debug(err.Type + " " + err.Tag + " " + err.Error.Error())
			return err.Error
		case <-timeoutCh:
			return errors.New("producer content timeout")
		default:
			if p.isConnected {
				timer.Stop()
				close(errCh)
				close(timeoutCh)
				p.NotifyConnErr(nil)
				if r.notifyErr != nil {
					p.NotifyConnErr(r.notifyErr)
				}
				break connectCheck
			} else {
				time.Sleep(time.Microsecond * 100)
				continue
			}
		}
	}
	r.pList[tag] = p
	return nil
}

func (r *RabbitMQ) DelProducer(tag string) {
	if _, ok := r.pList[tag]; ok {
		p := r.pList[tag]
		if p == nil {
			return
		}
		_ = p.Close()
		delete(r.pList, tag)
	}
}

func (r *RabbitMQ) Publish(tag string, exchange string, key string, body string) error {
	if _, ok := r.pList[tag]; !ok {
		return errors.New("p is not exists[" + tag + "]")
	}
	p := r.pList[tag]
	if p == nil {
		return errors.New("p is nil[" + tag + "]")
	}
	return p.Publish(exchange, key, body)
}

func (r *RabbitMQ) AddConsumer(tag string, queueName string, handler func(string)) error {
	if handler == nil {
		return errors.New("handle func can not be nil")
	}
	if _, ok := r.cList[tag]; ok {
		return errors.New("tag is already exists[" + tag + "]")
	}
	b, err := r.connectTest()
	if !b {
		errMsg := "test connect err:"
		if err != nil {
			return errors.New(errMsg + err.Error())
		} else {
			return errors.New(errMsg + "unknown error")
		}
	}
	c := &consumer{
		Tag:            tag,
		QueueName:      queueName,
		f:              handler,
		ConnStr:        r.getConnStr(),
		reConnectDelay: r.ReConnectDelay,
	}
	c.ctx, c.cancelFunc = context.WithCancel(context.Background())

	errCh := make(chan *RabbitMQError)
	c.NotifyConnErr(errCh)
	c.NotifyHandlerErr(errCh)
	go c.reConnection()
	go c.reStartHandler()

	timeoutCh := make(chan struct{})
	timer := time.AfterFunc(r.ConnectTimeout, func() {
		timeoutCh <- struct{}{}
	})

connectCheck:
	for {
		select {
		case err := <-errCh:
			log.Debug(err.Type + " " + err.Tag + " " + err.Error.Error())
			return err.Error
		case <-timeoutCh:
			return errors.New("producer content timeout")
		default:
			if c.isConnected && c.isHandled {
				timer.Stop()
				close(errCh)
				close(timeoutCh)
				c.NotifyHandlerErr(nil)
				c.NotifyConnErr(nil)
				if r.notifyErr != nil {
					c.NotifyConnErr(r.notifyErr)
					c.NotifyHandlerErr(r.notifyErr)
				}
				break connectCheck
			} else {
				time.Sleep(time.Millisecond * 100)
				continue
			}
		}
	}

	r.cList[tag] = c
	return nil
}

func (r *RabbitMQ) DelConsumer(tag string) {
	if _, ok := r.cList[tag]; ok {
		c := r.cList[tag]
		if c == nil {
			return
		}
		_ = c.Close()
		delete(r.cList, tag)
	}
}

func (r *RabbitMQ) Close() {
	for key := range r.pList {
		r.DelProducer(key)
	}
	for key := range r.cList {
		r.DelConsumer(key)
	}
	if r.notifyErr != nil {
		close(r.notifyErr)
	}
}
