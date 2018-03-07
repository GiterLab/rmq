package rmq

import (
	"crypto/sha1"
	"fmt"
	"os"
	"time"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

// Message is the application type for a message.
// This can contain identity, or a reference to the recevier chan
// for further demuxing.
type Message []byte

// CallBack callback function
type CallBack func(msg []byte)

// Session composes an amqp.Connection with an amqp.Channel
type Session struct {
	*amqp.Connection
	*amqp.Channel
}

// Close tears the connection down, taking the channel with it.
func (s Session) Close() error {
	if s.Connection == nil {
		return nil
	}
	return s.Connection.Close()
}

// Client rabbitmq客户端信息
type Client struct {
	URL                 string // rmqurl used to connect rabbitmq services
	Exchange            string // exchange binds the publishers to the subscribers
	ExchangeType        string // type of exchange
	QueueBindEnable     bool   // enable or disable queue bind
	QueueName           string // name of queue
	RoutingKey          string // routing key of queue
	Qos                 int    // qos of subscribe
	MsgExpirationEnable bool   // enable or disable message expiration
	ExpirationTime      int32  // expiration time of message
	PublishStatus       bool   // publish status

	Session *Session
}

// SetURL 设置客户端的连接信息
func (c *Client) SetURL(url, vhost, username, passwd string) {
	if vhost[0] != '/' {
		vhost = "/" + vhost
	}
	c.URL = "amqp://" + username + ":" + passwd + "@" + url + vhost
}

// SetExchange 设置交换器信息
func (c *Client) SetExchange(ex, exType string) {
	c.Exchange = ex
	c.ExchangeType = exType
}

// SetQueueBindEnable 设置是否需要声明和绑定到交换器
func (c *Client) SetQueueBindEnable(enable bool) {
	c.QueueBindEnable = enable
}

// SetQueueName 设置队列名称
func (c *Client) SetQueueName(q string) {
	c.QueueName = q
}

// SetRoutingKey 设置routingKey
func (c *Client) SetRoutingKey(rkey string) {
	c.RoutingKey = rkey
}

// SetQos 设置Qos值
func (c *Client) SetQos(qos int) {
	c.Qos = qos
}

// SetMsgExpirationEnable 设置消息是否过期
func (c *Client) SetMsgExpirationEnable(enable bool) {
	c.MsgExpirationEnable = enable
}

// SetExpirationTime 设置消息过期时间, 单位ms
func (c *Client) SetExpirationTime(time int32) {
	c.ExpirationTime = time
}

// SetPublishStatus 设置生产者状态
func (c *Client) SetPublishStatus(status bool) {
	c.PublishStatus = status
}

// GetPublishStatus 获取生产者状态
func (c *Client) GetPublishStatus() bool {
	return c.PublishStatus
}

// Info 客户端配置信息
func (c *Client) Info() {
	fmt.Println("rmq url:", c.URL)
	fmt.Println("rmq exchange:", c.Exchange)
	fmt.Println("rmq exchange type:", c.ExchangeType)
	fmt.Println("rmq queue bing enable:", c.QueueBindEnable)
	fmt.Println("rmq queue name:", c.QueueName)
	fmt.Println("rmq routing key:", c.RoutingKey)
	fmt.Println("rmq qos:", c.Qos)
	fmt.Println("rmq msg expiration enable:", c.MsgExpirationEnable)
	fmt.Println("rmq expiration time:", c.ExpirationTime)

	GLog.Info("[RMQ] info, url: %s", c.URL)
	GLog.Info("[RMQ] info, exchange: %s", c.Exchange)
	GLog.Info("[RMQ] info, exchange type: %s", c.ExchangeType)
	GLog.Info("[RMQ] info, queue bing enable: %q", c.QueueBindEnable)
	GLog.Info("[RMQ] info, queue name: %s", c.QueueName)
	GLog.Info("[RMQ] info, routing key: %s", c.RoutingKey)
	GLog.Info("[RMQ] info, qos: %d", c.Qos)
	GLog.Info("[RMQ] info, msg expiration enable: %q", c.MsgExpirationEnable)
	GLog.Info("[RMQ] info, expiration time: %d", c.ExpirationTime)
}

// Identity returns the same host/process unique string for the lifetime of
// this process so that subscriber reconnections reuse the same queue name.
func (c *Client) Identity() string {
	if c.Exchange == "" {
		fmt.Println("[RMQ] set exchange first")
		os.Exit(0)
	}

	if c.ExchangeType == "" {
		fmt.Println("[RMQ] set exchange type first")
		os.Exit(0)
	}

	// use user's queue name
	if c.QueueName != "" {
		return c.QueueName
	}

	// use system's queue name(auto gen from hostname)
	hostname, err := os.Hostname()
	h := sha1.New()
	fmt.Fprint(h, hostname)
	fmt.Fprint(h, err)
	fmt.Fprint(h, os.Getpid())
	return fmt.Sprintf("%x", h.Sum(nil))
}

// redial continually connects to the URL, exiting the program when no longer possible
func redial(ctx context.Context, c *Client) chan chan Session {
	if c == nil {
		fmt.Println("[RMQ] set client first")
		os.Exit(0)
	}
	c.SetPublishStatus(false)
	c.QueueName = c.Identity()
	sessions := make(chan chan Session)

	go func() {
		var conn *amqp.Connection
		var err error

		sess := make(chan Session)
		defer func() {
			if err := recover(); err != nil {
				GLog.Error("[RMQ] redial, go func defer, err: %s", err)
			}
			close(sess)
			close(sessions)
			c.SetPublishStatus(false)
		}()

		for {
			select {
			case sessions <- sess:
			case <-ctx.Done():
				GLog.Error("[RMQ] redial, shutting down session factory")
				// if the conn is not close, close it first
				if conn != nil {
					conn.Close()
				}
				return
			}

		redial_rmq:
			c.SetPublishStatus(false)
			time.Sleep(2 * time.Second)
			GLog.Info("[RMQ] redial, start to connect to rmq...")
			if conn != nil {
				conn.Close()
			}
			conn, err = amqp.Dial(c.URL)
			if err != nil {
				GLog.Error("[RMQ] redial, cannot (re)dial: %q, err: %s", c.URL, err)
				if conn != nil {
					conn.Close()
				}
				goto redial_rmq
			}

			ch, err := conn.Channel()
			if err != nil {
				GLog.Error("[RMQ] redial, cannot create channel, err: %s", err)
				if conn != nil {
					conn.Close()
				}
				goto redial_rmq
			}

			// 1. Declare exchange
			err = ch.ExchangeDeclare(
				c.Exchange,     // name
				c.ExchangeType, // type
				true,           // durable
				false,          // auto-deleted
				false,          // internal
				false,          // no-wait
				nil,            // arguments
			)
			if err != nil {
				GLog.Error("[RMQ] redial, cannot declare fanout exchange, err: %s", err)
				if conn != nil {
					conn.Close()
				}
				goto redial_rmq
			}

			if c.QueueBindEnable {
				// 设置消息过期时间
				// 默认7天
				arg := make(map[string]interface{})
				if c.ExpirationTime != 0 {
					arg["x-message-ttl"] = c.ExpirationTime
				} else {
					arg["x-message-ttl"] = int32(7 * 24 * 60 * 60 * 1000)
				}
				if !c.MsgExpirationEnable {
					arg = nil
				}

				// 2. Declare queue
				q, err := ch.QueueDeclare(
					c.QueueName, // name
					true,        // durable
					false,       // delete when unused
					false,       // exclusive
					false,       // no-wait
					arg,         // arguments
				)
				if err != nil {
					GLog.Error("[RMQ] redial, cannot consume from exclusive queue: %q, err: %s", c.QueueName, err)
					if conn != nil {
						conn.Close()
					}
					goto redial_rmq
				}

				// set routhing key, default is giterlab
				if c.RoutingKey == "" {
					c.RoutingKey = "giterlab"
				}

				// 3. Bind queue
				err = ch.QueueBind(
					q.Name,       // queue name
					c.RoutingKey, // routing key
					c.Exchange,   // exchange
					false,
					nil)
				if err != nil {
					GLog.Error("[RMQ] redial, cannot consume without a binding to exchange: %q, err: %s", c.Exchange, err)
					if conn != nil {
						conn.Close()
					}
					goto redial_rmq
				}
			}

			select {
			case sess <- Session{conn, ch}:
			case <-ctx.Done():
				GLog.Error("[RMQ] redial, shutting down new session")
				return
			}
		}
	}()

	return sessions
}
