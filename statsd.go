package statsd

import (
	"bufio"
	"fmt"
	. "github.com/visionmedia/go-debug"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"
)

var debug = Debug("statsd")

const defaultBufSize = 512

// Client is statsd client representing a connection to a statsd server.
type Client struct {
	conn   net.Conn
	buf    *bufio.Writer
	m      sync.Mutex
	prefix string
}

func millisecond(d time.Duration) int {
	return int(d.Seconds() * 1000)
}

// Dial connects to the given address on the given network using net.Dial and then returns a new Client for the connection.
func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return newClient(conn, 0), nil
}

// NewClient returns a new client with the given writer, useful for testing.
func NewClient(w io.Writer) *Client {
	return &Client{
		buf: bufio.NewWriterSize(w, defaultBufSize),
	}
}

// DialTimeout acts like Dial but takes a timeout. The timeout includes name resolution, if required.
func DialTimeout(addr string, timeout time.Duration) (*Client, error) {
	conn, err := net.DialTimeout("udp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return newClient(conn, 0), nil
}

// DialSize acts like Dial but takes a packet size.
// By default, the packet size is 512, see https://github.com/etsy/statsd/blob/master/docs/metric_types.md#multi-metric-packets for guidelines.
func DialSize(addr string, size int) (*Client, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return newClient(conn, size), nil
}

func newClient(conn net.Conn, size int) *Client {
	if size <= 0 {
		size = defaultBufSize
	}
	return &Client{
		conn: conn,
		buf:  bufio.NewWriterSize(conn, size),
	}
}

// Prefix adds a prefix to every stat string. The prefix is literal,
// so if you want "foo.bar.baz" from "baz" you should set the prefix
// to "foo.bar." not "foo.bar" as no delimiter is added for you.
func (c *Client) Prefix(s string) {
	c.prefix = s
}

// Increment increments the counter for the given bucket.
func (c *Client) Increment(stat string, count int, rate float64) error {
	return c.send(stat, rate, "%d|c", count)
}

// Incr increments the counter for the given bucket by 1 at a rate of 1.
func (c *Client) Incr(stat string) error {
	return c.Increment(stat, 1, 1)
}

// IncrBy increments the counter for the given bucket by N at a rate of 1.
func (c *Client) IncrBy(stat string, n int) error {
	return c.Increment(stat, n, 1)
}

// Decrement decrements the counter for the given bucket.
func (c *Client) Decrement(stat string, count int, rate float64) error {
	return c.Increment(stat, -count, rate)
}

// Decr decrements the counter for the given bucket by 1 at a rate of 1.
func (c *Client) Decr(stat string) error {
	return c.Increment(stat, -1, 1)
}

// DecrBy decrements the counter for the given bucket by N at a rate of 1.
func (c *Client) DecrBy(stat string, value int) error {
	return c.Increment(stat, -value, 1)
}

// Duration records time spent for the given bucket with time.Duration.
func (c *Client) Duration(stat string, duration time.Duration, rate float64) error {
	return c.send(stat, rate, "%d|ms", millisecond(duration))
}

// DurationSince records time spent for the given bucket since `t`.
func (c *Client) DurationSince(stat string, t time.Time) error {
	return c.send(stat, 1, "%d|ms", millisecond(time.Since(t)))
}

// Timing records time spent for the given bucket in milliseconds.
func (c *Client) Timing(stat string, delta int, rate float64) error {
	return c.send(stat, rate, "%d|ms", delta)
}

// Histogram is an alias of .Timing() until statsd implementations figure their shit out.
func (c *Client) Histogram(stat string, value int, rate float64) error {
	return c.send(stat, rate, "%d|ms", value)
}

// Time calculates time spent in given function and send it.
func (c *Client) Time(stat string, rate float64, f func()) error {
	ts := time.Now()
	f()
	return c.Duration(stat, time.Since(ts), rate)
}

// Gauge records arbitrary values for the given bucket.
func (c *Client) Gauge(stat string, value int, rate float64) error {
	return c.send(stat, rate, "%d|g", value)
}

// IncrementGauge increments the value of the gauge.
func (c *Client) IncrementGauge(stat string, value int, rate float64) error {
	return c.send(stat, rate, "+%d|g", value)
}

// IncrementGaugeBy increments the value of the gauge.
func (c *Client) IncrementGaugeBy(stat string, value int) error {
	return c.send(stat, 1, "+%d|g", value)
}

// DecrementGauge decrements the value of the gauge.
func (c *Client) DecrementGauge(stat string, value int, rate float64) error {
	return c.send(stat, rate, "-%d|g", value)
}

// DecrementGaugeBy decrements the value of the gauge.
func (c *Client) DecrementGaugeBy(stat string, value int) error {
	return c.send(stat, 1, "-%d|g", value)
}

// Unique records unique occurences of events.
func (c *Client) Unique(stat string, value int, rate float64) error {
	return c.send(stat, rate, "%d|s", value)
}

// Annotate sends an annotation.
func (c *Client) Annotate(name string, value string, args ...interface{}) error {
	return c.send(name, 1, "%s|a", fmt.Sprintf(value, args...))
}

// Flush flushes writes any buffered data to the network.
func (c *Client) Flush() error {
	return c.buf.Flush()
}

// Close closes the connection.
func (c *Client) Close() error {
	if err := c.Flush(); err != nil {
		return err
	}
	c.buf = nil
	return c.conn.Close()
}

func (c *Client) send(stat string, rate float64, format string, args ...interface{}) error {
	if c.prefix != "" {
		stat = c.prefix + stat
	}

	if rate < 1 {
		if rand.Float64() < rate {
			format = fmt.Sprintf("%s|@%g", format, rate)
		} else {
			return nil
		}
	}

	format = fmt.Sprintf("%s:%s", stat, format)
	debug(format, args...)

	c.m.Lock()
	defer c.m.Unlock()

	// Flush data if we have reach the buffer limit
	if c.buf.Available() < len(format) {
		if err := c.Flush(); err != nil {
			return nil
		}
	}

	// Buffer is not empty, start filling it
	if c.buf.Buffered() > 0 {
		format = fmt.Sprintf("\n%s", format)
	}

	_, err := fmt.Fprintf(c.buf, format, args...)
	return err
}
