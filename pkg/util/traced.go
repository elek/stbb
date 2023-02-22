package util

import (
	"context"
	"github.com/spacemonkeygo/monkit/v3"
	"storj.io/drpc"
)

var mon = monkit.Package()

type TracedConnection struct {
	Conn drpc.Conn
}

var _ drpc.Conn = &TracedConnection{}

func NewTracedConnection(conn drpc.Conn) drpc.Conn {
	return &TracedConnection{
		Conn: conn,
	}
}
func (t *TracedConnection) Close() error {
	return t.Conn.Close()
}

func (t *TracedConnection) Closed() <-chan struct{} {
	return t.Conn.Closed()
}

func (t *TracedConnection) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) (err error) {
	defer mon.Task(monkit.NewSeriesTag("rpc", rpc))(&ctx)(&err)
	return t.Conn.Invoke(ctx, rpc, enc, in, out)
}

func (t *TracedConnection) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error) {
	st, err := t.Conn.NewStream(ctx, rpc, enc)
	return &TracedStream{
		stream: st,
		rpc:    rpc,
	}, err
}

type TracedStream struct {
	stream drpc.Stream
	rpc    string
}

func (t *TracedStream) Context() context.Context {
	return t.stream.Context()
}

func (t *TracedStream) MsgSend(msg drpc.Message, enc drpc.Encoding) (err error) {
	ctx := t.stream.Context()
	defer mon.Task(monkit.NewSeriesTag("rpc", t.rpc))(&ctx)(&err)
	return t.stream.MsgSend(msg, enc)
}

func (t *TracedStream) MsgRecv(msg drpc.Message, enc drpc.Encoding) (err error) {
	ctx := t.stream.Context()
	defer mon.Task(monkit.NewSeriesTag("rpc", t.rpc))(&ctx)(&err)
	return t.stream.MsgRecv(msg, enc)
}

func (t *TracedStream) CloseSend() error {
	return t.stream.CloseSend()
}

func (t *TracedStream) Close() error {
	return t.stream.Close()
}

var _ drpc.Stream = &TracedStream{}
