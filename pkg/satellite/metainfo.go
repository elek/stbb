package satellite

import (
	"context"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"github.com/zeebo/errs"
	"golang.org/x/exp/slices"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/rpc/rpcstatus"
	"storj.io/common/signing"
	"storj.io/common/storj"
	"storj.io/common/uuid"
	"storj.io/storj/satellite/internalpb"
	"storj.io/storj/satellite/metainfo"
	"time"
)

type Metainfo struct {
	identity *identity.FullIdentity
}

func (m *Metainfo) DeleteObjects(ctx context.Context, request *pb.DeleteObjectsRequest) (*pb.DeleteObjectsResponse, error) {
	panic("implement me")
}

var _ pb.DRPCMetainfoServer = (*Metainfo)(nil)

func (m *Metainfo) SetBucketObjectLockConfiguration(ctx context.Context, request *pb.SetBucketObjectLockConfigurationRequest) (*pb.SetBucketObjectLockConfigurationResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) GetObjectLegalHold(ctx context.Context, request *pb.GetObjectLegalHoldRequest) (*pb.GetObjectLegalHoldResponse, error) {
	return &pb.GetObjectLegalHoldResponse{
		Enabled: false,
	}, nil
}

func (m *Metainfo) SetObjectLegalHold(ctx context.Context, request *pb.SetObjectLegalHoldRequest) (*pb.SetObjectLegalHoldResponse, error) {
	//TODO implement me
	panic("implement me")
}

func NewMetainfo(identity *identity.FullIdentity) *Metainfo {
	return &Metainfo{
		identity: identity,
	}
}

func (m *Metainfo) CreateBucket(ctx context.Context, request *pb.CreateBucketRequest) (*pb.CreateBucketResponse, error) {
	return &pb.CreateBucketResponse{
		Bucket: &pb.Bucket{
			Name: request.Name,
		},
	}, nil
}

func (m *Metainfo) GetBucket(ctx context.Context, request *pb.GetBucketRequest) (*pb.GetBucketResponse, error) {
	return &pb.GetBucketResponse{
		Bucket: &pb.Bucket{
			Name: request.Name,
		},
	}, nil
}

func (m *Metainfo) GetBucketLocation(ctx context.Context, request *pb.GetBucketLocationRequest) (*pb.GetBucketLocationResponse, error) {
	return &pb.GetBucketLocationResponse{
		Location: []byte("global"),
	}, nil
}

func (m *Metainfo) GetBucketVersioning(ctx context.Context, request *pb.GetBucketVersioningRequest) (*pb.GetBucketVersioningResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) SetBucketVersioning(ctx context.Context, request *pb.SetBucketVersioningRequest) (*pb.SetBucketVersioningResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) GetBucketObjectLockConfiguration(ctx context.Context, request *pb.GetBucketObjectLockConfigurationRequest) (*pb.GetBucketObjectLockConfigurationResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) DeleteBucket(ctx context.Context, request *pb.DeleteBucketRequest) (*pb.DeleteBucketResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) ListBuckets(ctx context.Context, request *pb.ListBucketsRequest) (*pb.ListBucketsResponse, error) {

	return &pb.ListBucketsResponse{
		Items: []*pb.BucketListItem{
			{
				Name:      []byte("nothing"),
				CreatedAt: time.Now(),
			},
		},
	}, nil
}

func (m *Metainfo) BeginObject(ctx context.Context, request *pb.BeginObjectRequest) (*pb.BeginObjectResponse, error) {
	streamID, err := uuid.New()
	if err != nil {
		return nil, rpcstatus.Error(rpcstatus.Internal, "unable to create stream id")
	}
	satStreamID, err := packStreamID(ctx, &internalpb.StreamID{
		Bucket:             []byte(request.Bucket),
		EncryptedObjectKey: []byte(request.EncryptedObjectKey),
		Version:            int64(request.Version),
		CreationDate:       time.Now(),
		StreamId:           streamID[:],
		MultipartObject:    false,
		//EncryptionParameters: encryptionParameters,
		Placement: int32(0),
		Versioned: false,
	}, m.identity)
	if err != nil {
		return nil, rpcstatus.Error(rpcstatus.Internal, "unable to create stream id")
	}

	return &pb.ObjectBeginResponse{
		Bucket:             request.Bucket,
		EncryptedObjectKey: request.EncryptedObjectKey,
		StreamId:           satStreamID,
		//RedundancyScheme:   endpoint.defaultRS,
	}, nil
}

func packStreamID(ctx context.Context, satStreamID *internalpb.StreamID, identiy *identity.FullIdentity) (streamID storj.StreamID, err error) {

	if satStreamID == nil {
		return nil, rpcstatus.Error(rpcstatus.Internal, "unable to create stream id")
	}

	if !satStreamID.ExpirationDate.IsZero() {
		// DB can only preserve microseconds precision and nano seconds will be cut.
		// To have stable StreamID/UploadID we need to always truncate it.
		satStreamID.ExpirationDate = satStreamID.ExpirationDate.Truncate(time.Microsecond)
	}

	signedStreamID, err := metainfo.SignStreamID(ctx, signing.SignerFromFullIdentity(identiy), satStreamID)
	if err != nil {
		return nil, rpcstatus.Error(rpcstatus.Internal, err.Error())
	}

	encodedStreamID, err := pb.Marshal(signedStreamID)
	if err != nil {
		return nil, rpcstatus.Error(rpcstatus.Internal, err.Error())
	}

	streamID, err = storj.StreamIDFromBytes(encodedStreamID)
	if err != nil {
		return nil, rpcstatus.Error(rpcstatus.Internal, err.Error())
	}
	return streamID, nil
}

func (m *Metainfo) CommitObject(ctx context.Context, request *pb.CommitObjectRequest) (*pb.CommitObjectResponse, error) {
	return &pb.CommitObjectResponse{
		Object: &pb.Object{},
	}, nil
}

func (m *Metainfo) GetObject(ctx context.Context, request *pb.GetObjectRequest) (*pb.GetObjectResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) ListObjects(ctx context.Context, request *pb.ListObjectsRequest) (*pb.ListObjectsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) BeginDeleteObject(ctx context.Context, request *pb.BeginDeleteObjectRequest) (*pb.BeginDeleteObjectResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) FinishDeleteObject(ctx context.Context, request *pb.FinishDeleteObjectRequest) (*pb.FinishDeleteObjectResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) GetObjectIPs(ctx context.Context, request *pb.GetObjectIPsRequest) (*pb.GetObjectIPsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) ListPendingObjectStreams(ctx context.Context, request *pb.ListPendingObjectStreamsRequest) (*pb.ListPendingObjectStreamsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) DownloadObject(ctx context.Context, request *pb.DownloadObjectRequest) (*pb.DownloadObjectResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) UpdateObjectMetadata(ctx context.Context, request *pb.UpdateObjectMetadataRequest) (*pb.UpdateObjectMetadataResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) GetObjectRetention(ctx context.Context, request *pb.GetObjectRetentionRequest) (*pb.GetObjectRetentionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) SetObjectRetention(ctx context.Context, request *pb.SetObjectRetentionRequest) (*pb.SetObjectRetentionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) BeginSegment(ctx context.Context, request *pb.BeginSegmentRequest) (*pb.BeginSegmentResponse, error) {
	return &pb.BeginSegmentResponse{}, nil
}

func (m *Metainfo) RetryBeginSegmentPieces(ctx context.Context, request *pb.RetryBeginSegmentPiecesRequest) (*pb.RetryBeginSegmentPiecesResponse, error) {
	return &pb.RetryBeginSegmentPiecesResponse{}, nil
}

func (m *Metainfo) CommitSegment(ctx context.Context, request *pb.CommitSegmentRequest) (*pb.CommitSegmentResponse, error) {
	return &pb.CommitSegmentResponse{}, nil
}

func (m *Metainfo) MakeInlineSegment(ctx context.Context, request *pb.MakeInlineSegmentRequest) (*pb.MakeInlineSegmentResponse, error) {
	return &pb.MakeInlineSegmentResponse{}, nil
}

func (m *Metainfo) BeginDeleteSegment(ctx context.Context, request *pb.BeginDeleteSegmentRequest) (*pb.BeginDeleteSegmentResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) FinishDeleteSegment(ctx context.Context, request *pb.FinishDeleteSegmentRequest) (*pb.FinishDeleteSegmentResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) ListSegments(ctx context.Context, request *pb.ListSegmentsRequest) (*pb.ListSegmentsResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) DownloadSegment(ctx context.Context, request *pb.DownloadSegmentRequest) (*pb.DownloadSegmentResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) DeletePart(ctx context.Context, request *pb.DeletePartRequest) (*pb.DeletePartResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) Batch(ctx context.Context, request *pb.BatchRequest) (*pb.BatchResponse, error) {
	resp := &pb.BatchResponse{}

	for _, r := range request.Requests {
		switch singleRequest := r.Request.(type) {
		case *pb.BatchRequestItem_ObjectBegin:
			r, err := m.BeginObject(ctx, singleRequest.ObjectBegin)
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &pb.BatchResponseItem{
				Response: &pb.BatchResponseItem_ObjectBegin{
					ObjectBegin: r,
				},
			})
		case *pb.BatchRequestItem_ObjectCommit:
			r, err := m.CommitObject(ctx, singleRequest.ObjectCommit)
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &pb.BatchResponseItem{
				Response: &pb.BatchResponseItem_ObjectCommit{
					ObjectCommit: r,
				},
			})
		case *pb.BatchRequestItem_SegmentMakeInline:
			r, err := m.MakeInlineSegment(ctx, singleRequest.SegmentMakeInline)
			if err != nil {
				return nil, err
			}
			resp.Responses = append(resp.Responses, &pb.BatchResponseItem{
				Response: &pb.BatchResponseItem_SegmentMakeInline{
					SegmentMakeInline: r,
				},
			})
		default:
			panic(fmt.Sprintf("unsupported %T", singleRequest))
		}
	}
	return resp, nil
}

func (m *Metainfo) CompressedBatch(ctx context.Context, request *pb.CompressedBatchRequest) (*pb.CompressedBatchResponse, error) {
	var err error
	zr, err := zstd.NewReader(nil)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	zw, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	var reqData []byte
	switch request.Selected {
	case pb.CompressedBatchRequest_NONE:
		reqData = request.Data
	case pb.CompressedBatchRequest_ZSTD:
		reqData, err = zr.DecodeAll(request.Data, nil)
	default:
		err = errs.New("unsupported compression")
	}
	if err != nil {
		return nil, errs.Wrap(err)
	}
	var unReq pb.BatchRequest
	err = pb.Unmarshal(reqData, &unReq)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	unResp, err := m.Batch(ctx, &unReq)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	unrespData, err := pb.Marshal(unResp)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	resp := new(pb.CompressedBatchResponse)
	if slices.Contains(request.Supported, pb.CompressedBatchRequest_ZSTD) {
		resp.Data = zw.EncodeAll(unrespData, nil)
		resp.Selected = pb.CompressedBatchRequest_ZSTD
	} else {
		resp.Data = unrespData
		resp.Selected = pb.CompressedBatchRequest_NONE
	}
	return resp, nil

}

//
//	var unReq pb.BatchRequest
//	err = pb.Unmarshal(reqData, &unReq)
//	if err != nil {
//		return nil, errs.Wrap(err)
//	}
//
//	unResp, err := endpoint.Batch(ctx, &unReq)
//	if err != nil {
//		return nil, errs.Wrap(err)
//	}
//
//	unrespData, err := pb.Marshal(unResp)
//	if err != nil {
//		return nil, errs.Wrap(err)
//	}
//
//	resp = new(pb.CompressedBatchResponse)
//	if slices.Contains(req.Supported, pb.CompressedBatchRequest_ZSTD) {
//		resp.Data = endpoint.zstdEncoder.EncodeAll(unrespData, nil)
//		resp.Selected = pb.CompressedBatchRequest_ZSTD
//	} else {
//		resp.Data = unrespData
//		resp.Selected = pb.CompressedBatchRequest_NONE
//	}
//}

func (m *Metainfo) ProjectInfo(ctx context.Context, request *pb.ProjectInfoRequest) (*pb.ProjectInfoResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) RevokeAPIKey(ctx context.Context, request *pb.RevokeAPIKeyRequest) (*pb.RevokeAPIKeyResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) BeginMoveObject(ctx context.Context, request *pb.BeginMoveObjectRequest) (*pb.BeginMoveObjectResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) FinishMoveObject(ctx context.Context, request *pb.FinishMoveObjectRequest) (*pb.FinishMoveObjectResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) BeginCopyObject(ctx context.Context, request *pb.BeginCopyObjectRequest) (*pb.BeginCopyObjectResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (m *Metainfo) FinishCopyObject(ctx context.Context, request *pb.FinishCopyObjectRequest) (*pb.FinishCopyObjectResponse, error) {
	//TODO implement me
	panic("implement me")
}
