package rmq

import (
	"golang.org/x/net/context"
)

// Publish rabbitmq publish
func Publish(messages chan Message, c *Client) {
	go func(messages chan Message, c *Client) {
		for {
			ctx, done := context.WithCancel(context.Background())
			go func(messages chan Message, c *Client) {
				defer func() {
					// recover panic
					if err := recover(); err != nil {
						TraceError("[RMQ] Publish panic, go func defer, err: %s", err)
					}
					done()
				}()
				publish(redial(ctx, c), messages, messages, c)
				TraceInfo("[RMQ] Publish reconnect...")
			}(messages, c)
			<-ctx.Done()
		}
	}(messages, c)
}

// PublishWithRoutingKey rabbitmq publish
func PublishWithRoutingKey(messages chan MessageWithRoutingKey, c *Client) {
	go func(messages chan MessageWithRoutingKey, c *Client) {
		for {
			ctx, done := context.WithCancel(context.Background())
			go func(messages chan MessageWithRoutingKey, c *Client) {
				defer func() {
					// recover panic
					if err := recover(); err != nil {
						TraceError("[RMQ] PublishWithRoutingKey panic, go func defer, err: %s", err)
					}
					done()
				}()
				publishWithRoutingKey(redial(ctx, c), messages, messages, c)
				TraceInfo("[RMQ] PublishWithRoutingKey reconnect...")
			}(messages, c)
			<-ctx.Done()
		}
	}(messages, c)
}
