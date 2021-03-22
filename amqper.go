package amqper

import (
	"fmt"

	"github.com/streadway/amqp"
)

// NewWorker returns main worker base
func NewWorker(config *Configuration, processor RunnableConsumer) (*Worker, chan error) {
	if config.QueueArguments == nil {
		config.QueueArguments = make(map[string]interface{})
	}
	if config.DelayedQueueArguments == nil {
		config.QueueArguments = make(map[string]interface{})
	}

	wrk := &Worker{
		processorStorage: processor,
		config:           config,
	}

	wrk.shutdownCh = make(chan bool, 1)
	wrk.errorCh = make(chan error)

	if wrk.config.AsyncWorker {

		if wrk.config.AsyncPoolSize == 0 {
			wrk.config.AsyncPoolSize = defaultAsyncPoolSize
		}

		wrk.amqpMessagesPoolCh = make(chan *amqp.Delivery, config.AsyncPoolSize)

		wrk.runnerForProcessor = wrk.processorRunnerAsync
	} else {
		wrk.runnerForProcessor = wrk.processorRunnerSync
	}

	wrk.amqpQueueDelayedName = fmt.Sprintf("%s-delayed", config.QueueName)

	return wrk, wrk.errorCh
}

// Serve start worker processing messages
func (wrk *Worker) Serve() error {

	var err error
	err = wrk.initAmqpConnection()
	if err != nil {
		return err
	}

	err = wrk.initAmqpChannel()
	if err != nil {
		return err
	}

	err = wrk.initChannelQos()
	if err != nil {
		return err
	}

	if wrk.config.UseDelayedQueue {

		err = wrk.initDelayedQueue()
		if err != nil {
			return err
		}
	}

	err = wrk.initQueue()
	if err != nil {
		return err
	}

	err = wrk.initConsumer()
	if err != nil {
		return err
	}

	for { // worker loop
		select {
		case err := <-wrk.amqpNotifyCloseConnection:
			// work with error
			return wrk.errorStop(err)
		case msg := <-wrk.amqpMessages:
			// work with message
			wrk.runnerForProcessor(&msg)
		case msg := <-wrk.amqpMessagesPoolCh:
			// work if async mode with pool
			go wrk.processMessage(msg)
			// case <-wrk.shutdownCh:
			// work with graceful stop
			// wrk.graceStop()
		}
	}
}

// Close is for graceful serve stop
func (wrk *Worker) Close() error {
	// wrk.shutdownCh <- true
	return wrk.graceStop()
}

func (wrk *Worker) graceStop() error {
	if err := wrk.amqpChannel.Close(); err != nil {
		return fmt.Errorf("error with graceful close channel: %s", err)
	}
	if err := wrk.amqpConnection.Close(); err != nil {
		return fmt.Errorf("error with graceful close connection: %s", err)
	}
	return nil
}

func (wrk *Worker) errorStop(err error) error {
	if err != nil {
		return fmt.Errorf("lost connection with AMQP: %v", err)
	}
	return nil
}

func (wrk *Worker) initAmqpConnection() error {
	var err error

	wrk.amqpConnection, err = amqp.DialConfig(wrk.config.ConnectionString, wrk.config.ConnectionConfig)
	if err != nil {
		return fmt.Errorf("%s: %s", "failed to connect to AMQP", err)
	}
	wrk.amqpNotifyCloseConnection = wrk.amqpConnection.NotifyClose(make(chan *amqp.Error))
	return nil
}

func (wrk *Worker) initAmqpChannel() error {
	var err error
	wrk.amqpChannel, err = wrk.amqpConnection.Channel()
	if err != nil {
		return fmt.Errorf("%s: %s", "failed to open a channel", err)
	}
	return nil
}

func (wrk *Worker) initChannelQos() error {
	var err error
	err = wrk.amqpChannel.Qos(wrk.config.PrefetchCount, 0, false)
	if err != nil {
		return fmt.Errorf("%s: %s", "failed to set channel QoS", err)
	}
	return nil
}

func (wrk *Worker) initDelayedQueue() error {
	var err error
	if wrk.config.DelayedQueueArguments == nil {
		wrk.config.DelayedQueueArguments = make(map[string]interface{})
	}
	wrk.config.DelayedQueueArguments["x-dead-letter-exchange"] = ""
	wrk.config.DelayedQueueArguments["x-dead-letter-routing-key"] = wrk.config.QueueName
	wrk.amqpQueueDelayed, err = wrk.amqpChannel.QueueDeclare(
		wrk.amqpQueueDelayedName,         // name
		wrk.config.QueueDurable,          // durable
		false,                            // delete when usused
		false,                            // exclusive
		false,                            // no-wait
		wrk.config.DelayedQueueArguments, // arguments
	)
	if err != nil {
		return fmt.Errorf("%s: %s", "failed to declare a delayed queue", err)
	}
	return nil
}

func (wrk *Worker) initQueue() error {
	var err error
	wrk.amqpQueue, err = wrk.amqpChannel.QueueDeclare(
		wrk.config.QueueName,       // name
		wrk.config.QueueDurable,    // durable
		wrk.config.QueueAutoDelete, // delete when usused
		wrk.config.QueueExclusive,  // exclusive
		wrk.config.QueueNoWait,     // no-wait
		wrk.config.QueueArguments,  // arguments
	)
	if err != nil {
		return fmt.Errorf("%s: %s", "failed to declare a queue", err)
	}
	return nil
}

func (wrk *Worker) initConsumer() error {
	var err error
	wrk.amqpMessages, err = wrk.amqpChannel.Consume(
		wrk.config.QueueName,      // queue
		"",                        // consumer
		wrk.config.QueueAutoACK,   // auto-ack
		wrk.config.QueueExclusive, // exclusive
		false,                     // no-local
		wrk.config.QueueNoWait,    // no-wait
		nil,                       // args
	)
	if err != nil {
		return fmt.Errorf("%s: %s", "failed to init consume", err)
	}
	return nil
}
