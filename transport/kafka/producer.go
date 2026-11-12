package kafka

import (
	"context"
	"sync"

	"github.com/IBM/sarama"

	"github.com/prabhatdotdev/weave/core"
)

type messageProducer interface {
	SendMessageContext(context.Context, *sarama.ProducerMessage) (int32, int64, error)
	Close() error
}

type publishResult struct {
	partition int32
	offset    int64
	err       error
}

type contextProducer struct {
	producer sarama.AsyncProducer
	closing  chan struct{}
	done     chan struct{}
	mu       sync.RWMutex
	once     sync.Once
}

func newProducer(brokers []string, config *sarama.Config) (messageProducer, error) {
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	return wrapProducer(producer), nil
}

func wrapProducer(producer sarama.AsyncProducer) *contextProducer {
	p := &contextProducer{producer: producer, closing: make(chan struct{}), done: make(chan struct{})}
	go p.collectResults()
	return p
}

func (p *contextProducer) collectResults() {
	defer close(p.done)
	successes, failures := p.producer.Successes(), p.producer.Errors()
	for successes != nil || failures != nil {
		select {
		case message, ok := <-successes:
			if !ok {
				successes = nil
				continue
			}
			message.Metadata.(chan publishResult) <- publishResult{partition: message.Partition, offset: message.Offset}
		case failure, ok := <-failures:
			if !ok {
				failures = nil
				continue
			}
			failure.Msg.Metadata.(chan publishResult) <- publishResult{err: failure.Err}
		}
	}
}

func (p *contextProducer) SendMessageContext(ctx context.Context, message *sarama.ProducerMessage) (int32, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	result := make(chan publishResult, 1)
	message.Metadata = result
	p.mu.RLock()
	select {
	case <-p.closing:
		p.mu.RUnlock()
		return 0, 0, &core.ErrConnectionLost{Backend: backendName}
	default:
	}
	select {
	case p.producer.Input() <- message:
		p.mu.RUnlock()
	case <-ctx.Done():
		p.mu.RUnlock()
		return 0, 0, ctx.Err()
	case <-p.closing:
		p.mu.RUnlock()
		return 0, 0, &core.ErrConnectionLost{Backend: backendName}
	}
	select {
	case received := <-result:
		if ctx.Err() != nil {
			return 0, 0, ctx.Err()
		}
		return received.partition, received.offset, received.err
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	case <-p.closing:
		return 0, 0, &core.ErrConnectionLost{Backend: backendName}
	}
}

func (p *contextProducer) Close() error {
	p.once.Do(func() {
		close(p.closing)
		p.mu.Lock()
		p.producer.AsyncClose()
		p.mu.Unlock()
	})
	<-p.done
	return nil
}
