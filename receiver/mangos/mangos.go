package mangos

import (
	"bytes"
	"go.nanomsg.org/mangos/v3"
	"net"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/go-graphite/go-carbon/helper"
	"github.com/go-graphite/go-carbon/points"
	"github.com/go-graphite/go-carbon/receiver"
	"github.com/go-graphite/go-carbon/receiver/parse"
	"github.com/lomik/zapwriter"
	"github.com/prometheus/client_golang/prometheus"

	"go.nanomsg.org/mangos/v3/protocol/pull"
	// register transports
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

func init() {
	receiver.Register(
		"mangos",
		func() interface{} { return NewOptions() },
		func(name string, options interface{}, store func(*points.Points)) (receiver.Receiver, error) {
			return newMANGOS(name, options.(*Options), store)
		},
	)
}

type Options struct {
	Listen  string `toml:"url"`
	Enabled bool   `toml:"enabled"`
}

// MANGOS receive metrics from mangos socket
type MANGOS struct {
	helper.Stoppable
	out             func(*points.Points)
	name            string
	metricsReceived uint32
	errors          uint32
	logIncomplete   bool //nolint:unused,structcheck
	sock            mangos.Socket
	buffer          chan *points.Points
	logger          *zap.Logger
}

func NewOptions() *Options {
	return &Options{
		Listen:  "tcp://0.0.0.0:2003",
		Enabled: true,
	}
}

// Addr returns binded socket address. For bind port 0 in tests
func (rcv *MANGOS) Addr() net.Addr {
	// TODO implement
	return nil
}

func newMANGOS(name string, options *Options, store func(*points.Points)) (*MANGOS, error) {
	if !options.Enabled {
		return nil, nil
	}

	var sock mangos.Socket
	var err error
	sock, err = pull.NewSocket()
	if err != nil {
		return nil, err
	}
	if err = sock.Listen(options.Listen); err != nil {
		return nil, err
	}

	r := &MANGOS{
		sock:   sock,
		out:    store,
		name:   name,
		logger: zapwriter.Logger(name),
	}

	err = r.Listen()
	if err != nil {
		return nil, err
	}

	return r, err
}

func (rcv *MANGOS) Stat(send helper.StatCallback) {
	metricsReceived := atomic.LoadUint32(&rcv.metricsReceived)
	atomic.AddUint32(&rcv.metricsReceived, -metricsReceived)
	send("metricsReceived", float64(metricsReceived))

	errors := atomic.LoadUint32(&rcv.errors)
	atomic.AddUint32(&rcv.errors, -errors)
	send("errors", float64(errors))

	if rcv.buffer != nil {
		send("bufferLen", float64(len(rcv.buffer)))
		send("bufferCap", float64(cap(rcv.buffer)))
	}
}

func (rcv *MANGOS) receiveWorker(chan bool) {
	defer rcv.sock.Close()

	var data *bytes.Buffer

	for {
		msg, err := rcv.sock.Recv()
		if err != nil {
			atomic.AddUint32(&rcv.errors, 1)
			rcv.logger.Error("read error", zap.Error(err))
			continue
		}

		data = bytes.NewBuffer(msg)

		for {
			line, err := data.ReadBytes('\n')

			if len(line) > 0 {
				name, value, timestamp, err := parse.PlainLine(line)
				if err != nil {
					atomic.AddUint32(&rcv.errors, 1)
					rcv.logger.Debug("parse failed",
						zap.Error(err),
						zap.String("peer", rcv.Addr().String()),
					)
				} else {
					atomic.AddUint32(&rcv.metricsReceived, 1)
					rcv.out(points.OnePoint(string(name), value, timestamp))
				}
			}

			if err != nil {
				break
			}
		}
	}
}

// Listen mangos socket. Receive messages and send to out channel
func (rcv *MANGOS) Listen() error {
	return rcv.StartFunc(func() error {
		rcv.Go(func(exit chan bool) {
			<-exit
			rcv.sock.Close()
		})

		if rcv.buffer != nil {
			originalOut := rcv.out

			rcv.Go(func(exit chan bool) {
				for {
					select {
					case <-exit:
						return
					case p := <-rcv.buffer:
						originalOut(p)
					}
				}
			})

			rcv.out = func(p *points.Points) {
				rcv.buffer <- p
			}
		}

		rcv.Go(rcv.receiveWorker)

		return nil
	})
}

// InitPrometheus is a stub for the receiver prom metrics. Required to satisfy Receiver interface.
func (*MANGOS) InitPrometheus(prometheus.Registerer) {
}
