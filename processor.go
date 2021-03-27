package amqper

import (
	"fmt"
	"strconv"
	"time"

	"github.com/rb-pkg/amqp"
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

	pub := amqp.AcquirePublishing()
	defer amqp.ReleasePublishing(pub)

	pub.Headers = amqpMSG.Headers
	pub.ContentType = amqpMSG.ContentType
	pub.ContentEncoding = amqpMSG.ContentEncoding
	pub.DeliveryMode = amqpMSG.DeliveryMode
	pub.Priority = amqpMSG.Priority
	pub.CorrelationId = amqpMSG.CorrelationId
	pub.ReplyTo = amqpMSG.ReplyTo
	pub.Expiration = strconv.FormatInt(int64(delay/time.Millisecond), 10)
	pub.MessageId = amqpMSG.MessageId
	pub.Timestamp = time.Now()
	pub.Type = amqpMSG.Type
	pub.UserId = amqpMSG.UserId
	pub.AppId = amqpMSG.AppId
	pub.Body = amqpMSG.Body

	return wrk.amqpChannel.Publish("", wrk.amqpQueueDelayedName, false, false, pub)
}
