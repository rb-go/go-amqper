package workercore

import (
	"github.com/streadway/amqp"
)

func (wrk *Worker) processorRunnerAsync(msg *amqp.Delivery) {
	wrk.amqpMessagesPoolCh <- msg
}

func (wrk *Worker) processorRunnerSync(msg *amqp.Delivery) {
	wrk.processMessage(msg)
}
