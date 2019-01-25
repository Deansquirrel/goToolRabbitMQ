package goToolRabbitMQ

import (
	"context"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

const(
	connStrFormat = "amqp://%s:%s@%s:%d/%s"

	ReConnectDelay = time.Second * 30
	ReSendDelay = time.Microsecond * 500
	ReSendTimes = uint32(3)
	WaitConfirmTimeout = time.Second * 5
	ConnectTimeout = time.Second * 30
)

const(
	ExchangeDirect = amqp.ExchangeDirect
	ExchangeFanout = amqp.ExchangeFanout
	ExchangeHeaders = amqp.ExchangeHeaders
	ExchangeTopic = amqp.ExchangeTopic
)

type RabbitMQ struct {
	connection *amqp.Connection

	config *RabbitMQConfig

	ReConnectDelay time.Duration
	ReSendDelay time.Duration
	ReSendTimes uint32
	WaitConfirmTimeout time.Duration
	ConnectTimeout time.Duration

	pList map[string]*producer
	cList map[string]*consumer
}

type RabbitMQConfig struct {
	Server string
	Port int
	VirtualHost string
	User string
	Password string
}

func NewRabbitMQ(config *RabbitMQConfig) (*RabbitMQ,error){
	r := &RabbitMQ{
		config:config,

		ReConnectDelay:ReConnectDelay,
		ReSendDelay:ReSendDelay,
		ReSendTimes:ReSendTimes,
		WaitConfirmTimeout:WaitConfirmTimeout,
		ConnectTimeout:ConnectTimeout,

		pList:make(map[string]*producer),
		cList:make(map[string]*consumer),
	}
	b,err := r.connectTest()
	if !b {
		return nil,err
	}
	conn,err := r.getConn()
	if err != nil {
		return nil,err
	}
	r.connection = conn
	return r,nil
}

func (r *RabbitMQ) getConn() (*amqp.Connection,error) {
	conn, err := amqp.Dial(r.getConnStr())
	if err != nil {
		return nil,err
	}
	return conn,nil
}

func (r *RabbitMQ)getConnStr()string{
	return fmt.Sprintf(connStrFormat, r.config.User, r.config.Password, r.config.Server, r.config.Port,r.config.VirtualHost)
}

func (r *RabbitMQ)getChannel() (*amqp.Channel,error) {
	ch,err := r.connection.Channel()
	if err != nil {
		return nil,err
	}
	return ch,nil
}

func (r *RabbitMQ)ExchangeDeclareSimple(exchange, exchangeType string) error {
	return r.ExchangeDeclare(exchange,exchangeType,true,false,false,false)
}

func (r *RabbitMQ)ExchangeDeclare(exchange, exchangeType string, durable, autoDelete, internal, noWait bool) error {
	ch,err := r.getChannel()
	if err != nil {
		return err
	}
	defer func(){
		_ = ch.Close()
	}()
	err = ch.ExchangeDeclare(exchange,exchangeType,durable, autoDelete, internal, noWait, nil)
	return err
}

func (r *RabbitMQ)ExchangeDelete(exchange string,ifUnused, noWait bool) error {
	ch,err := r.getChannel()
	if err != nil {
		return err
	}
	defer func(){
		_ = ch.Close()
	}()
	return ch.ExchangeDelete(exchange,false,true)
}

func (r *RabbitMQ)QueueDeclareSimple(queue string) error{
	return r.QueueDeclare(queue,true,false,false,false)
}

func (r *RabbitMQ)QueueDeclare(queue string, durable, autoDelete, exclusive, noWait bool) error{
	ch,err := r.getChannel()
	if err != nil {
		return err
	}
	defer func(){
		_ = ch.Close()
	}()
	_,err = ch.QueueDeclare(queue,durable, autoDelete, exclusive, noWait,nil)
	return err
}

func (r *RabbitMQ)QueueDelete(queue string, ifUnused, ifEmpty, noWait bool) error{
	ch,err := r.getChannel()
	if err != nil {
		return err
	}
	defer func(){
		_ = ch.Close()
	}()
	_,err = ch.QueueDelete(queue,ifUnused,ifEmpty,noWait)
	return err
}

func (r *RabbitMQ)QueueBind(queueName, key, exchange string, noWait bool) error {
	ch,err := r.getChannel()
	if err != nil {
		return err
	}
	defer func(){
		_ = ch.Close()
	}()
	err = ch.QueueBind(queueName,key,exchange,noWait,nil)
	return err
}

func (r *RabbitMQ)QueueUnbind(queueName, key, exchange string) error {
	ch,err := r.getChannel()
	if err != nil {
		return err
	}
	defer func(){
		_ = ch.Close()
	}()
	err = ch.QueueUnbind(queueName,key,exchange,nil)
	return err
}

func (r *RabbitMQ)connectTest()(bool,error) {
	conn,err := amqp.Dial(r.getConnStr())
	if err != nil {
		return false,err
	}
	defer func(){
		_ = conn.Close()
	}()
	ch,err := conn.Channel()
	if err != nil {
		return false,err
	}
	defer func(){
		_ = ch.Close()
	}()
	return true,nil
}

func (r *RabbitMQ)AddProducer(tag string)error{
	if _,ok := r.pList[tag];ok{
		return errors.New("tag is already exists[" + tag + "]")
	}

	b,err := r.connectTest()
	if !b{
		errMsg := "test connect err:"
		if err != nil {
			return errors.New(errMsg + err.Error())
		} else {
			return errors.New(errMsg + "unknown error")
		}
	}

	p := &producer{
		Tag:tag,
		ConnStr:r.getConnStr(),
		reConnectDelay:r.ReConnectDelay,
		reSendDelay:r.ReSendDelay,
		waitConfirmTimeout:r.WaitConfirmTimeout,
	}
	p.ctx,p.cancelFunc = context.WithCancel(context.Background())
	go p.reConnection()
	bTime := time.Now()
	for{
		if p.lastConnErr != nil {
			connErr := p.lastConnErr
			p.cancelFunc()
			_ = p.Close()
			return connErr
		}
		cTime := time.Now()
		if cTime.After(bTime.Add(r.ConnectTimeout)){
			return errors.New("producer content timeout")
		}
		if p.isConnected {
			break
		} else {
			time.Sleep(time.Microsecond * 100)
		}
	}
	r.pList[tag] = p
	return nil
}

func (r *RabbitMQ)DelProducer(tag string) {
	if _, ok := r.pList[tag]; ok {
		p := r.pList[tag]
		if p == nil {
			return
		}
		_ = p.Close()
		delete(r.pList, tag)
	}
}

func (r *RabbitMQ)Publish(tag string,exchange string, key string, body string) error{
	if _,ok := r.pList[tag];!ok{
		return errors.New("p is not exists[" + tag + "]")
	}
	p := r.pList[tag]
	if p == nil {
		return errors.New("p is nil[" + tag + "]")
	}
	return p.Publish(exchange,key,body)
}

func (r *RabbitMQ)AddConsumer(tag string,queueName string,handler func(string))error{
	if handler == nil {
		return errors.New("handle func can not be nil")
	}
	if _,ok := r.cList[tag];ok{
		return errors.New("tag is already exists[" + tag + "]")
	}
	b,err := r.connectTest()
	if !b{
		errMsg := "test connect err:"
		if err != nil {
			return errors.New(errMsg + err.Error())
		} else {
			return errors.New(errMsg + "unknown error")
		}
	}
	c := &consumer{
		Tag:tag,
		QueueName:queueName,
		f: handler,
		ConnStr:r.getConnStr(),
		reConnectDelay:r.ReConnectDelay,
	}
	c.ctx,c.cancelFunc = context.WithCancel(context.Background())
	go c.reConnection()
	go c.reStartHandler()
	r.cList[tag] = c
	return nil
}

func (r *RabbitMQ)DelConsumer(tag string){
	if _,ok := r.cList[tag];ok{
		c := r.cList[tag]
		if c == nil {
			return
		}
		_ = c.Close()
		delete(r.cList, tag)
	}
}

func (r *RabbitMQ)Close(){
	for key := range r.pList{
		r.DelProducer(key)
	}
	for key := range r.cList{
		r.DelConsumer(key)
	}
}