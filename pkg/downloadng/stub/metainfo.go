package stub

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/zeebo/errs"
	"path/filepath"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/drpc"
	"sync"
	"time"
)

var metainfoStub = &metainfo{
	StorageNodes: NewStubNodes(120),
	closed:       make(chan struct{}),
	segments:     make(map[string]storedSegment),
	objects:      make(map[string]storedObject),
	keys:         make(map[string]storj.StreamID),
}

type metainfo struct {
	StorageNodes stubNodes
	closed       chan struct{}
	once         sync.Once

	// segmentId --> segment
	segments map[string]storedSegment

	// streamID --> object
	objects map[string]storedObject

	// bucket/key... -> streamID
	keys map[string]storj.StreamID
}

type storedSegment struct {
	beginSegment  *pb.SegmentBeginRequest
	commitSegment *pb.SegmentCommitRequest
}

type storedObject struct {
	beginObject  *pb.ObjectBeginRequest
	commitObject *pb.ObjectCommitRequest
	segments     []storj.SegmentID
}

func (p *metainfo) Unblocked() <-chan struct{} {
	return make(chan struct{})
}

func (p *metainfo) Close() error {
	p.once.Do(func() {
		close(p.closed)
	})
	return nil
}

func (p *metainfo) Closed() <-chan struct{} {
	return p.closed
}

func (p *metainfo) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error {
	switch rpc {
	case "/metainfo.Metainfo/CreateBucket":
		req := in.(*pb.CreateBucketRequest)
		out = &pb.CreateBucketResponse{
			Bucket: &pb.Bucket{
				Name: req.Name,
			},
		}
		return nil
	case "/metainfo.Metainfo/BeginObject":
		req := in.(*pb.ObjectBeginRequest)
		resp := out.(*pb.ObjectBeginResponse)
		resp.Bucket = req.Bucket
		resp.EncryptedObjectKey = req.EncryptedObjectKey
		resp.StreamId = storj.StreamID{}
		_, err := rand.Read(resp.StreamId[:])
		if err != nil {
			panic(err)
		}
		p.objects[resp.StreamId.String()] = storedObject{
			beginObject: req,
			segments:    make([]storj.SegmentID, 0),
		}
		return nil
	case "/metainfo.Metainfo/SegmentBegin":
		req := in.(*pb.SegmentBeginRequest)
		resp := out.(*pb.SegmentBeginResponse)
		var limits []*pb.AddressedOrderLimit
		for i := 0; i < 20; i++ {
			limits = append(limits, &pb.AddressedOrderLimit{
				Limit: &pb.OrderLimit{
					StorageNodeId: p.StorageNodes[i].Identity.ID,
					Limit:         1024,
				},
				StorageNodeAddress: &pb.NodeAddress{
					Address: p.StorageNodes[i].Address,
				},
			})
		}

		_, key, err := storj.NewPieceKey()
		if err != nil {
			return err
		}
		resp.AddressedLimits = limits
		resp.PrivateKey = key
		resp.RedundancyScheme = &pb.RedundancyScheme{
			Type:             pb.RedundancyScheme_RS,
			MinReq:           10,
			Total:            20,
			RepairThreshold:  11,
			SuccessThreshold: 20,
			ErasureShareSize: 1024,
		}
		resp.SegmentId = pb.SegmentID{}
		p.segments[resp.SegmentId.String()] = storedSegment{
			beginSegment: req,
		}
		return nil
	case "/metainfo.Metainfo/SegmentCommit":
		req := in.(*pb.SegmentCommitRequest)
		segment := p.segments[req.SegmentId.String()]
		segment.commitSegment = req
		object := p.objects[segment.beginSegment.StreamId.String()]
		object.segments = append(object.segments, req.SegmentId)
		return nil
	case "/metainfo.Metainfo/ObjectCommit":
		req := in.(*pb.ObjectCommitRequest)
		o := p.objects[req.StreamId.String()]
		p.keys[filepath.Join(string(o.beginObject.Bucket), string(o.beginObject.EncryptedObjectKey))] = req.StreamId
		return nil
	case "/metainfo.Metainfo/DownloadObject":
		req := in.(*pb.DownloadObjectRequest)
		resp := out.(*pb.DownloadObjectResponse)
		streamID, found := p.keys[filepath.Join(string(req.Bucket), string(req.EncryptedObjectKey))]
		if !found {
			return errs.New("Couldn't find key %s/%X", req.Bucket, req.EncryptedObjectKey)
		}
		o := p.objects[streamID.String()]

		resp.Object = &pb.Object{
			Bucket:             o.beginObject.Bucket,
			EncryptedObjectKey: o.beginObject.EncryptedObjectKey,
			CreatedAt:          time.Now(),
			TotalSize:          1024,
			RemoteSize:         1024,
			EncryptionParameters: &pb.EncryptionParameters{
				CipherSuite: pb.CipherSuite_ENC_AESGCM,
				BlockSize:   128,
			},
			RedundancyScheme: &pb.RedundancyScheme{
				Type:             pb.RedundancyScheme_RS,
				MinReq:           10,
				Total:            20,
				RepairThreshold:  16,
				SuccessThreshold: 12,
				ErasureShareSize: 128,
			},
			StreamId: storj.StreamID{},
		}
		resp.SegmentDownload = []*pb.DownloadSegmentResponse{}

		resp.SegmentList = &pb.ListSegmentsResponse{
			Items: make([]*pb.SegmentListItem, 0),
		}

		for _, segmentID := range o.segments {
			resp.SegmentList.Items = append(resp.SegmentList.Items, &pb.SegmentListItem{
				Position: &pb.SegmentPosition{
					PartNumber: 0,
				},
			})

			limits := make([]*pb.AddressedOrderLimit, 0)
			for _, u := range p.segments[segmentID.String()].commitSegment.UploadResult {
				node, err := p.StorageNodes.GetByID(u.NodeId)
				if err != nil {
					panic("missing node " + u.NodeId.String())
				}
				limits = append(limits, &pb.AddressedOrderLimit{
					Limit: &pb.OrderLimit{
						SerialNumber:       storj.SerialNumber{},
						StorageNodeId:      node.Identity.ID,
						UplinkPublicKey:    storj.PiecePublicKey{},
						PieceId:            storj.PieceID{},
						Limit:              int64(1024),
						OrderExpiration:    time.Now().Add(24 * time.Hour),
						Action:             pb.PieceAction_GET,
						SatelliteSignature: []byte{},
					},
					StorageNodeAddress: nil,
				})
			}
			resp.SegmentDownload = append(resp.SegmentDownload, &pb.SegmentDownloadResponse{
				SegmentId:       segmentID,
				AddressedLimits: limits,
			})
		}

		return nil

	case "/metainfo.Metainfo/Batch":
		req := in.(*pb.BatchRequest)
		br := out.(*pb.BatchResponse)
		for _, r := range req.Requests {
			if r.GetObjectBegin() != nil {
				resp := &pb.BatchResponseItem_ObjectBegin{
					ObjectBegin: &pb.ObjectBeginResponse{},
				}
				err := p.Invoke(ctx, "/metainfo.Metainfo/BeginObject", enc, r.GetObjectBegin(), resp.ObjectBegin)
				if err != nil {
					return err
				}
				br.Responses = append(br.Responses, &pb.BatchResponseItem{
					Response: resp,
				})
			} else if r.GetSegmentBegin() != nil {
				resp := &pb.BatchResponseItem_SegmentBegin{
					SegmentBegin: &pb.SegmentBeginResponse{},
				}
				err := p.Invoke(ctx, "/metainfo.Metainfo/SegmentBegin", enc, r.GetSegmentBegin(), resp.SegmentBegin)
				if err != nil {
					return err
				}
				br.Responses = append(br.Responses, &pb.BatchResponseItem{
					Response: resp,
				})
			} else if r.GetSegmentCommit() != nil {
				resp := &pb.BatchResponseItem_SegmentCommit{
					SegmentCommit: &pb.SegmentCommitResponse{},
				}
				err := p.Invoke(ctx, "/metainfo.Metainfo/SegmentCommit", enc, r.GetSegmentCommit(), resp.SegmentCommit)
				if err != nil {
					return err
				}
				br.Responses = append(br.Responses, &pb.BatchResponseItem{
					Response: resp,
				})
			} else if r.GetObjectCommit() != nil {
				resp := &pb.BatchResponseItem_ObjectCommit{
					ObjectCommit: &pb.ObjectCommitResponse{},
				}
				err := p.Invoke(ctx, "/metainfo.Metainfo/ObjectCommit", enc, r.GetObjectCommit(), resp.ObjectCommit)
				if err != nil {
					return err
				}
				br.Responses = append(br.Responses, &pb.BatchResponseItem{
					Response: resp,
				})
			} else {
				panic(fmt.Sprintf("handler for batch type is not implemented: %s", r))
			}
		}
		return nil
	default:
		panic(fmt.Sprintf("%s is not supported", rpc))
	}
}

func (p *metainfo) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error) {
	panic("not implemented")
}

var _ drpc.Conn = &metainfo{}
