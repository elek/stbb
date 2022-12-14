// Code generated by protoc-gen-go-drpc. DO NOT EDIT.
// protoc-gen-go-drpc version: v0.0.32
// source: sesamestreet.proto

package rpc

import (
	context "context"
	errors "errors"
	protojson "google.golang.org/protobuf/encoding/protojson"
	proto "google.golang.org/protobuf/proto"
	drpc "storj.io/drpc"
	drpcerr "storj.io/drpc/drpcerr"
)

type drpcEncoding_File_sesamestreet_proto struct{}

func (drpcEncoding_File_sesamestreet_proto) Marshal(msg drpc.Message) ([]byte, error) {
	return proto.Marshal(msg.(proto.Message))
}

func (drpcEncoding_File_sesamestreet_proto) MarshalAppend(buf []byte, msg drpc.Message) ([]byte, error) {
	return proto.MarshalOptions{}.MarshalAppend(buf, msg.(proto.Message))
}

func (drpcEncoding_File_sesamestreet_proto) Unmarshal(buf []byte, msg drpc.Message) error {
	return proto.Unmarshal(buf, msg.(proto.Message))
}

func (drpcEncoding_File_sesamestreet_proto) JSONMarshal(msg drpc.Message) ([]byte, error) {
	return protojson.Marshal(msg.(proto.Message))
}

func (drpcEncoding_File_sesamestreet_proto) JSONUnmarshal(buf []byte, msg drpc.Message) error {
	return protojson.Unmarshal(buf, msg.(proto.Message))
}

type DRPCCookieMonsterClient interface {
	DRPCConn() drpc.Conn

	EatCookie(ctx context.Context) (DRPCCookieMonster_EatCookieClient, error)
}

type drpcCookieMonsterClient struct {
	cc drpc.Conn
}

func NewDRPCCookieMonsterClient(cc drpc.Conn) DRPCCookieMonsterClient {
	return &drpcCookieMonsterClient{cc}
}

func (c *drpcCookieMonsterClient) DRPCConn() drpc.Conn { return c.cc }

func (c *drpcCookieMonsterClient) EatCookie(ctx context.Context) (DRPCCookieMonster_EatCookieClient, error) {
	stream, err := c.cc.NewStream(ctx, "/sesamestreet.CookieMonster/EatCookie", drpcEncoding_File_sesamestreet_proto{})
	if err != nil {
		return nil, err
	}
	x := &drpcCookieMonster_EatCookieClient{stream}
	return x, nil
}

type DRPCCookieMonster_EatCookieClient interface {
	drpc.Stream
	Send(*Cookie) error
	Recv() (*Crumbs, error)
}

type drpcCookieMonster_EatCookieClient struct {
	drpc.Stream
}

func (x *drpcCookieMonster_EatCookieClient) Send(m *Cookie) error {
	return x.MsgSend(m, drpcEncoding_File_sesamestreet_proto{})
}

func (x *drpcCookieMonster_EatCookieClient) Recv() (*Crumbs, error) {
	m := new(Crumbs)
	if err := x.MsgRecv(m, drpcEncoding_File_sesamestreet_proto{}); err != nil {
		return nil, err
	}
	return m, nil
}

func (x *drpcCookieMonster_EatCookieClient) RecvMsg(m *Crumbs) error {
	return x.MsgRecv(m, drpcEncoding_File_sesamestreet_proto{})
}

type DRPCCookieMonsterServer interface {
	EatCookie(DRPCCookieMonster_EatCookieStream) error
}

type DRPCCookieMonsterUnimplementedServer struct{}

func (s *DRPCCookieMonsterUnimplementedServer) EatCookie(DRPCCookieMonster_EatCookieStream) error {
	return drpcerr.WithCode(errors.New("Unimplemented"), drpcerr.Unimplemented)
}

type DRPCCookieMonsterDescription struct{}

func (DRPCCookieMonsterDescription) NumMethods() int { return 1 }

func (DRPCCookieMonsterDescription) Method(n int) (string, drpc.Encoding, drpc.Receiver, interface{}, bool) {
	switch n {
	case 0:
		return "/sesamestreet.CookieMonster/EatCookie", drpcEncoding_File_sesamestreet_proto{},
			func(srv interface{}, ctx context.Context, in1, in2 interface{}) (drpc.Message, error) {
				return nil, srv.(DRPCCookieMonsterServer).
					EatCookie(
						&drpcCookieMonster_EatCookieStream{in1.(drpc.Stream)},
					)
			}, DRPCCookieMonsterServer.EatCookie, true
	default:
		return "", nil, nil, nil, false
	}
}

func DRPCRegisterCookieMonster(mux drpc.Mux, impl DRPCCookieMonsterServer) error {
	return mux.Register(impl, DRPCCookieMonsterDescription{})
}

type DRPCCookieMonster_EatCookieStream interface {
	drpc.Stream
	Send(*Crumbs) error
	Recv() (*Cookie, error)
}

type drpcCookieMonster_EatCookieStream struct {
	drpc.Stream
}

func (x *drpcCookieMonster_EatCookieStream) Send(m *Crumbs) error {
	return x.MsgSend(m, drpcEncoding_File_sesamestreet_proto{})
}

func (x *drpcCookieMonster_EatCookieStream) Recv() (*Cookie, error) {
	m := new(Cookie)
	if err := x.MsgRecv(m, drpcEncoding_File_sesamestreet_proto{}); err != nil {
		return nil, err
	}
	return m, nil
}

func (x *drpcCookieMonster_EatCookieStream) RecvMsg(m *Cookie) error {
	return x.MsgRecv(m, drpcEncoding_File_sesamestreet_proto{})
}
