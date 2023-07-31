package stub

import (
	"context"
	"fmt"
	"storj.io/common/pb"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"storj.io/drpc"
	"sync"
	"time"
)

var nodes = map[storj.NodeID]*nodeContent{}

var nodesLock = sync.Mutex{}

type nodeContent struct {
	pieces map[storj.PieceID][]byte
}

type DownloadState struct {
	initialOffset int64
	maxPaidOffset int64
	position      int64
}

type piecestore struct {
	node          *nodeStub
	closed        chan struct{}
	once          sync.Once
	key           storj.PiecePrivateKey
	pieceSet      map[int][]byte
	pieceTemplate []byte
	nodeContent   *nodeContent
}

func (p *piecestore) Unblocked() <-chan struct{} {
	return make(chan struct{})
}

func NewPiecestoreStub(node *nodeStub) (*piecestore, error) {
	_, key, err := storj.NewPieceKey()
	if err != nil {
		panic(err)
	}
	nodesLock.Lock()
	defer nodesLock.Unlock()
	if _, found := nodes[node.Identity.ID]; !found {
		nodes[node.Identity.ID] = &nodeContent{
			pieces: make(map[storj.PieceID][]byte),
		}
	}

	return &piecestore{
		key:         key,
		node:        node,
		closed:      make(chan struct{}),
		nodeContent: nodes[node.Identity.ID],
	}, nil
}

func (p *piecestore) Close() error {
	p.once.Do(func() {
		close(p.closed)
	})
	return nil
}

func (p *piecestore) Closed() <-chan struct{} {
	return p.closed
}

func (p *piecestore) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error {
	panic("implement me")
}

func (p *piecestore) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error) {
	return &piecestoreStream{
		key:         p.key,
		rpc:         rpc,
		ctx:         ctx,
		node:        p.node,
		requests:    make(chan drpc.Message, 1000),
		downloads:   make(map[string]*DownloadState),
		nodeContent: p.nodeContent,
	}, nil
}

type piecestoreStream struct {
	ctx         context.Context
	node        *nodeStub
	rpc         string
	requests    chan drpc.Message
	key         storj.PiecePrivateKey
	downloads   map[string]*DownloadState
	nodeContent *nodeContent
}

func (p *piecestoreStream) Context() context.Context {
	return p.ctx
}

func (p *piecestoreStream) MsgSend(msg drpc.Message, enc drpc.Encoding) error {
	switch m := msg.(type) {
	case *pb.PieceUploadRequest:
		if m.Limit == nil {
			return nil
		}
		if _, found := p.nodeContent.pieces[m.Limit.PieceId]; !found {
			p.nodeContent.pieces[m.Limit.PieceId] = []byte{}
		}
		if m.Chunk != nil && m.Chunk.Data != nil {
			p.nodeContent.pieces[m.Limit.PieceId] = append(p.nodeContent.pieces[m.Limit.PieceId], m.Chunk.Data...)
		}
		if m.Done == nil {
			return nil
		}
	case *pb.PieceDownloadRequest:
		key := p.getStreamKey(m)
		downloadState, found := p.downloads[key]
		if !found {
			downloadState = &DownloadState{}
			p.downloads[key] = downloadState
		}
		if m.Order != nil {
			downloadState.maxPaidOffset = downloadState.initialOffset + m.Order.Amount
		}
		if m.Chunk != nil {
			downloadState.initialOffset = m.Chunk.Offset
			downloadState.position = m.Chunk.Offset
			return nil
		}

	default:
		panic(fmt.Sprintf("%T is not supported", m))
	}
	p.requests <- msg
	return nil
}

func (p *piecestoreStream) getStreamKey(m *pb.PieceDownloadRequest) string {
	key := ""
	if m.Limit != nil {
		key = m.Limit.SerialNumber.String()
	}
	if m.Order != nil {
		key = m.Order.SerialNumber.String()
	}
	return key
}

func (p *piecestoreStream) MsgRecv(msg drpc.Message, enc drpc.Encoding) error {
	request := <-p.requests

	switch m := request.(type) {
	case *pb.PieceUploadRequest:
		response := msg.(*pb.PieceUploadResponse)
		if m.Done != nil {
			signer := signing.SignerFromFullIdentity(p.node.Identity)
			m.Done.Timestamp = time.Now()
			hash, err := signing.SignPieceHash(p.ctx, signer, m.Done)
			if err != nil {
				return err
			}
			response.Done = hash
		}
	case *pb.PieceDownloadRequest:
		response := msg.(*pb.PieceDownloadResponse)
		key := p.getStreamKey(m)
		downloadState := p.downloads[key]
		remaining := downloadState.maxPaidOffset - downloadState.position
		if remaining > 0 {
			data := p.nodeContent.pieces[m.Limit.PieceId]
			response.Chunk = &pb.PieceDownloadResponse_Chunk{
				Offset: downloadState.position,
				Data:   data[downloadState.position : downloadState.position+remaining],
			}
			downloadState.position += remaining
		}
	case struct{}:
	default:
		panic(fmt.Sprintf("%T is not supported", m))
	}
	return nil
}

func (p *piecestoreStream) CloseSend() error {
	p.requests <- struct{}{}
	return nil
}

func (p *piecestoreStream) Close() error {
	if p.requests != nil {
		close(p.requests)
		p.requests = nil
	}
	return nil
}

var _ drpc.Conn = &piecestore{}
