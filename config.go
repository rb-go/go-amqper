package workercore

import (
	"time"

	"github.com/riftbit/golif"
	"github.com/streadway/amqp"
)

// Configuration is basic configureation for worker
type Configuration struct {
	ConnectionString      string
	ConnectionConfig      amqp.Config
	ExchangeName          string
	QueueName             string
	QueueAutoDelete       bool
	QueueExclusive        bool
	QueueNoWait           bool
	QueueAutoACK          bool
	PrefetchCount         int
	UseDelayedQueue       bool
	AsyncWorker           bool
	DefaultRetryCount     int32
	DefaultRetryDelay     int64
	QueueDurable          bool
	Heartbeat             time.Duration
	Locale                string
	QueueArguments        map[string]interface{}
	DelayedQueueArguments map[string]interface{}
}

// SendPushFunction type that define function that used to send push
type ProcessFunction func(amqpMSG *amqp.Delivery) (retryCnt *int32, retryDelay *int64, err error)

// Worker basic client
type Worker struct {
	config                    *Configuration
	logger                    golif.Logger
	amqpConnection            *amqp.Connection
	amqpChannel               *amqp.Channel
	amqpQueue                 amqp.Queue
	amqpQueueDelayed          amqp.Queue
	amqpMessages              <-chan amqp.Delivery
	amqpNotifyCloseConnection chan *amqp.Error
	amqpQueueDelayedName      string
	shutdownCh                chan bool
	processFunction           ProcessFunction
}
