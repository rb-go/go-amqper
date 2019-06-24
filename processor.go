package workercore

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

func (wrk *Worker) processMessage(amqpMSG *amqp.Delivery) {
	retryCnt, retryDelay, err := wrk.processorStorage.ProcessQueueTask(amqpMSG)
	if err != nil {
		if wrk.config.UseDelayedQueue {
			var repeatCnt int32
			if retryCnt != 0 {
				repeatCnt = retryCnt
			} else {
				repeatCnt = wrk.config.DefaultRetryCount
			}

			if repeatCnt > 0 {
				var currentTryID int32
				var nextTryID int32

				if amqpMSG.Headers == nil {
					amqpMSG.Headers = make(map[string]interface{})
				}

				if n, ok := amqpMSG.Headers["x-retry-id"].(int32); ok {
					currentTryID = n
					nextTryID = currentTryID + 1
				} else {
					currentTryID = 1
					nextTryID = currentTryID + 1
				}

				if currentTryID < repeatCnt {

					var delay time.Duration

					if retryDelay != 0 {
						delay = retryDelay
					} else {
						delay = wrk.config.DefaultRetryDelay
					}

					err = wrk.repubToDelayed(amqpMSG, nextTryID, delay)
					if err != nil {
						wrk.errorCh <- fmt.Errorf("cannot repub with delay: %v", err)
					}
				}
			}
		}

		err = amqpMSG.Nack(false, false)
		if err != nil {
			wrk.errorCh <- fmt.Errorf("cannot send NAck: %v", err)
		}
		return
	}

	err = amqpMSG.Ack(false)
	if err != nil {
		wrk.errorCh <- fmt.Errorf("cannot send Ack for delivery tag %d: %v", amqpMSG.DeliveryTag, err)
		return
	}
}

func (wrk *Worker) repubToDelayed(amqpMSG *amqp.Delivery, retryID int32, delay time.Duration) error {
	amqpMSG.Headers["x-retry-id"] = retryID
	return wrk.amqpChannel.Publish("", wrk.amqpQueueDelayedName, false, false, amqp.Publishing{
		Headers:         amqpMSG.Headers,
		ContentType:     amqpMSG.ContentType,
		ContentEncoding: amqpMSG.ContentEncoding,
		DeliveryMode:    amqpMSG.DeliveryMode,
		Priority:        amqpMSG.Priority,
		CorrelationId:   amqpMSG.CorrelationId,
		ReplyTo:         amqpMSG.ReplyTo,
		Expiration:      fmt.Sprintf("%d", int64(delay/time.Millisecond)),
		MessageId:       amqpMSG.MessageId,
		Timestamp:       time.Now(),
		Type:            amqpMSG.Type,
		UserId:          amqpMSG.UserId,
		AppId:           amqpMSG.AppId,
		Body:            amqpMSG.Body,
	})
}
