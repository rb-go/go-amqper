package amqper

import (
	"time"

	"github.com/streadway/amqp"
)

const (
	defaultAsyncPoolSize = 1
)

// Configuration is basic configuration for worker
type Configuration struct {
	AsyncWorker           bool
	AsyncPoolSize         int32
	ConnectionString      string
	ConnectionConfig      amqp.Config
	ExchangeName          string
	QueueName             string
	QueueDurable          bool
	QueueArguments        map[string]interface{}
	QueueAutoDelete       bool
	QueueExclusive        bool
	QueueNoWait           bool
	QueueAutoACK          bool
	PrefetchCount         int
	UseDelayedQueue       bool
	DelayedQueueArguments map[string]interface{}
	DefaultRetryCount     int32
	DefaultRetryDelay     time.Duration
}

// RunnableConsumer interface for processing function
type RunnableConsumer interface {
	ProcessQueueTask(amqpMSG *amqp.Delivery) (retryCnt int32, retryDelay time.Duration, err error)
}

// Worker basic client
type Worker struct {
	config                    *Configuration
	amqpConnection            *amqp.Connection
	amqpChannel               *amqp.Channel
	amqpQueue                 amqp.Queue
	amqpQueueDelayed          amqp.Queue
	amqpQueueDelayedName      string
	amqpMessages              <-chan amqp.Delivery
	amqpNotifyCloseConnection chan *amqp.Error
	shutdownCh                chan bool
	errorCh                   chan error
	amqpMessagesPoolCh        chan *amqp.Delivery
	runnerForProcessor        func(*amqp.Delivery)
	processorStorage          RunnableConsumer
}
