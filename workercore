package workercore

import (
	"fmt"

	"github.com/riftbit/golif"
	"github.com/streadway/amqp"
)

// NewWorker returns main worker base
func NewWorker(config *Configuration, logger golif.Logger) *Worker {
	if config.QueueArguments == nil {
		config.QueueArguments = make(map[string]interface{})
	}
	if config.DelayedQueueArguments == nil {
		config.QueueArguments = make(map[string]interface{})
	}
	wrk := &Worker{
		config: config,
		logger: logger,
	}
	wrk.amqpQueueDelayedName = fmt.Sprintf("%s-delayed", config.QueueName)
	return wrk
}

// Initialize inits worker connections and setup processing methods
func (wrk *Worker) Initialize(processFunction ProcessFunction) error {
	wrk.processFunction = processFunction

	wrk.shutdownCh = make(chan bool, 1)

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

	return nil
}

// Serve start worker processing msgs
func (wrk *Worker) Serve() error {

	for { // worker loop
		select {
		case err := <-wrk.amqpNotifyCloseConnection:
			// work with error
			return wrk.errorStop(err)
		case msg := <-wrk.amqpMessages:
			// work with message
			// wrk.logger.Printf("message received from rabbit %s with delivery tag %d", msg.ConsumerTag, msg.DeliveryTag)
			if wrk.config.AsyncWorker {
				go wrk.processMessage(&msg)
			} else {
				wrk.processMessage(&msg)
			}
		case <-wrk.shutdownCh:
			// work with graceful stop
			wrk.graceStop()
		}
	}
}

func (wrk *Worker) Close() {
	wrk.shutdownCh <- true
}

func (wrk *Worker) graceStop() {
	wrk.logger.Infof("shutdown signal received")

	if err := wrk.amqpChannel.Close(); err != nil {
		wrk.logger.Fatalf("error with graceful close channel: %s", err)
	}
	if err := wrk.amqpConnection.Close(); err != nil {
		wrk.logger.Fatalf("error with graceful close connection: %s", err)
	}

	wrk.logger.Println("server stopped")
}

func (wrk *Worker) errorStop(err error) error {
	if err != nil {
		wrk.logger.Fatalf("lost connection with rabbitmq: %v", err)
		return err
	}
	return nil
}

func (wrk *Worker) initAmqpConnection() error {
	var err error

	wrk.amqpConnection, err = amqp.DialConfig(wrk.config.ConnectionString, wrk.config.ConnectionConfig)

	if err != nil {
		return fmt.Errorf("%s: %s", "failed to connect to RabbitMQ", err)
	}

	wrk.amqpNotifyCloseConnection = wrk.amqpConnection.NotifyClose(make(chan *amqp.Error))

	wrk.logger.Printf("success connection to %s", wrk.config.ConnectionString)
	return nil
}

func (wrk *Worker) initAmqpChannel() error {
	var err error
	wrk.amqpChannel, err = wrk.amqpConnection.Channel()

	if err != nil {
		return fmt.Errorf("%s: %s", "failed to open a channel", err)
	}

	wrk.logger.Printf("success opening a channel")
	return nil
}

func (wrk *Worker) initDelayedQueue() error {
	var err error

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

	wrk.logger.Printf("success delayed queue declaration: %s", wrk.amqpQueueDelayedName)
	return nil
}

func (wrk *Worker) initChannelQos() error {
	var err error
	err = wrk.amqpChannel.Qos(wrk.config.PrefetchCount, 0, false)

	if err != nil {
		return fmt.Errorf("%s: %s", "failed to set QoS", err)
	}

	wrk.logger.Printf("setup QOS success")
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

	wrk.logger.Printf("success queue declaration: %s", wrk.config.QueueName)
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
		return fmt.Errorf("%s: %s", "failed to declare a queue", err)
	}

	wrk.logger.Printf("success consumer declaration: %s", wrk.amqpQueue.Name)
	return nil
}
