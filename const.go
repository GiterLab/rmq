package rmq

// Constants for standard AMQP 0-9-1 exchange types.
const (
	ExchangeDirect  = "direct"
	ExchangeFanout  = "fanout"
	ExchangeTopic   = "topic"
	ExchangeHeaders = "headers"
)

// DeliveryMode
// Transient means higher throughput but messages will not be
// restored on broker restart.  The delivery mode of publishings is unrelated
// to the durability of the queues they reside on.  Transient messages will
// not be restored to durable queues, persistent messages will be restored to
// durable queues and lost on non-durable queues during server restart.
//
// This remains typed as uint8 to match Publishing.DeliveryMode.  Other
// delivery modes specific to custom queue implementations are not enumerated
// here.
const (
	Transient  uint8 = 1
	Persistent uint8 = 2
)

// QueueType
// Classic queues are the standard queue type in RabbitMQ. They are backed by
// memory and disk, and can be replicated across a cluster of brokers.
// Quorum queues are replicated across a cluster of brokers, and are designed
// to be more durable than classic queues. They are backed by Raft, and are
// designed to be more resilient to network partitions and broker failures.
// Stream queues are append-only logs that are replicated across a cluster of
// brokers. They are designed for high-throughput, low-latency use cases.
const (
	QueueTypeClassic = "classic"
	QueueTypeQuorum  = "quorum"
	QueueTypeStream  = "stream"
)
