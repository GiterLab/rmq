package rmq

import (
	"golang.org/x/net/context"
)

// Subscribe rabbitmq subscribe
func Subscribe(handle CallBack, c *Client) {
	go func(handle CallBack, c *Client) {
		for {
			ctx, done := context.WithCancel(context.Background())
			go func(handle CallBack, c *Client) {
				defer func() {
					// recover panic
					if err := recover(); err != nil {
						TraceError("[RMQ] Subscribe panic, go func defer, err: %s", err)
					}
					done()
				}()
				subscribe(redial(ctx, c), handle, c)
				TraceInfo("[RMQ] Subscribe reconnect...")
			}(handle, c)
			<-ctx.Done()
		}
	}(handle, c)
}
