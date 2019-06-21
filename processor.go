package workercore

import (
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

func (wrk *Worker) processMessage(amqpMSG *amqp.Delivery) {

	retryCnt, retryDelay, err := wrk.processFunction(wrk.externalService, amqpMSG)

	if err != nil {

		if wrk.config.UseDelayedQueue {

			var repeatCnt int32

			if retryCnt != 0 {
				repeatCnt = retryCnt
			} else {
				repeatCnt = wrk.config.DefaultRetryCount
			}

			if repeatCnt > 0 {

				var currentTryId int32
				var nextTryId int32

				if amqpMSG.Headers == nil {
					amqpMSG.Headers = make(map[string]interface{})
				}

				if n, ok := amqpMSG.Headers["x-retry-id"].(int32); ok {
					currentTryId = n
					nextTryId = currentTryId + 1
				} else {
					currentTryId = 1
					nextTryId = currentTryId + 1
				}

				amqpMSG.Headers["x-retry-id"] = nextTryId

				if currentTryId < repeatCnt {

					var delay int64

					if retryDelay != 0 {
						delay = retryDelay
					} else {
						delay = wrk.config.DefaultRetryDelay
					}

					err = wrk.repubToDelayed(amqpMSG, nextTryId, delay)
					if err != nil {
						wrk.logger.Errorf("cannot repub with delay: %v", err)
					} else {
						wrk.logger.Infof("sended repub with that had delivery tag: %d and next try id %d and delay %d", amqpMSG.DeliveryTag, nextTryId, delay)
					}
				}
			}
		}

		err = amqpMSG.Nack(false, false)
		if err != nil {
			wrk.logger.Errorf("cannot send NAck: %v", err)
		} else {
			wrk.logger.Infof("deleted as unsuccess message with delivery tag %d", amqpMSG.DeliveryTag)
		}

		return
	}

	err = amqpMSG.Ack(false)
	if err != nil {
		wrk.logger.Errorf("cannot send Ack for delivery tag $d: %v", amqpMSG.DeliveryTag, err)
		return
	}
}

func (wrk *Worker) repubToDelayed(amqpMSG *amqp.Delivery, retryID int32, delay time.Duration) error {

	wrk.logger.Debugf("republishing to %s with delay %dms and retry-id %d", wrk.amqpQueueDelayedName, delay.Nanoseconds()/1e6, retryID)

	return wrk.amqpChannel.Publish("", wrk.amqpQueueDelayedName, false, false, amqp.Publishing{
		Headers:         amqpMSG.Headers,
		ContentType:     amqpMSG.ContentType,
		ContentEncoding: amqpMSG.ContentEncoding,
		DeliveryMode:    amqpMSG.DeliveryMode,
		Priority:        amqpMSG.Priority,
		CorrelationId:   amqpMSG.CorrelationId,
		ReplyTo:         amqpMSG.ReplyTo,
		Expiration:      fmt.Sprintf("%d", delay.Nanoseconds()/1e6),
		MessageId:       amqpMSG.MessageId,
		Timestamp:       time.Now(),
		Type:            amqpMSG.Type,
		UserId:          amqpMSG.UserId,
		AppId:           amqpMSG.AppId,
		Body:            amqpMSG.Body,
	})
}
