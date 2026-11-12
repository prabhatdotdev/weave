package amqp

import (
	"context"
	"fmt"

	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/prabhatdotdev/weave/core"
)

type confirmChannel interface {
	Confirm(bool) error
	NotifyPublish(chan amqplib.Confirmation) chan amqplib.Confirmation
	NotifyReturn(chan amqplib.Return) chan amqplib.Return
	NotifyClose(chan *amqplib.Error) chan *amqplib.Error
	PublishWithContext(context.Context, string, string, bool, bool, amqplib.Publishing) error
}

type confirmedPublisher struct {
	channel  confirmChannel
	confirms <-chan amqplib.Confirmation
	returns  <-chan amqplib.Return
	closed   <-chan *amqplib.Error
	// shortcut: one outstanding publish per channel; pipeline receipts when measured throughput requires it.
	gate chan struct{}
}

func newConfirmedPublisher(channel confirmChannel) (*confirmedPublisher, error) {
	p := &confirmedPublisher{
		channel:  channel,
		confirms: channel.NotifyPublish(make(chan amqplib.Confirmation, 1)),
		returns:  channel.NotifyReturn(make(chan amqplib.Return, 1)),
		closed:   channel.NotifyClose(make(chan *amqplib.Error, 1)),
		gate:     make(chan struct{}, 1),
	}
	if err := channel.Confirm(false); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *confirmedPublisher) publish(ctx context.Context, exchange, key string, mandatory, immediate bool, message amqplib.Publishing) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case p.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closed:
		return &core.ErrConnectionLost{Backend: backendName}
	}
	result := make(chan error, 1)
	go func() {
		defer func() { <-p.gate }()
		err := ctx.Err()
		if err == nil {
			err = p.channel.PublishWithContext(ctx, exchange, key, mandatory, immediate, message)
		}
		if err == nil {
			// Keep the gate until the receipt arrives, even after caller cancellation,
			// so late confirms and returns cannot be attributed to the next publish.
			select {
			case confirmation, ok := <-p.confirms:
				if !ok {
					err = &core.ErrConnectionLost{Backend: backendName}
				} else if !confirmation.Ack {
					err = core.ErrPublishRejected
				}
				// RabbitMQ sends basic.return before its confirmation. Both listeners
				// are buffered, so a returned message is available when its ack arrives.
				select {
				case returned, ok := <-p.returns:
					if ok {
						err = fmt.Errorf("%w: %d %s", core.ErrUnroutable, returned.ReplyCode, returned.ReplyText)
					}
				default:
				}
			case <-p.closed:
				err = &core.ErrConnectionLost{Backend: backendName}
			}
		}
		result <- err
	}()
	select {
	case err := <-result:
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closed:
		return &core.ErrConnectionLost{Backend: backendName}
	}
}
