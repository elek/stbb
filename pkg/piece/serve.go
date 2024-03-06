package piece

import (
	"context"
	"crypto/tls"
	_ "embed"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
	"net"
	"storj.io/common/experiment"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/common/rpc/rpctracing"
	"storj.io/drpc/drpcmigrate"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
	jaeger "storj.io/monkit-jaeger"
)

var (
	//go:embed identity.cert
	Cert []byte

	//go:embed identity.key
	Key []byte
)

func init() {
	cmd := &cobra.Command{
		Use: "serve",
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return serve()
	}
	PieceCmd.AddCommand(cmd)
}

func serve() error {
	ctx := context.Background()

	ident, err := identity.FullIdentityFromPEM(Cert, Key)
	fmt.Println("Starting ", ident.ID.String()+"@localhost:28567")
	if err != nil {
		return errs.Wrap(err)
	}

	tlsConfig := tlsopts.Config{
		UsePeerCAWhitelist: false,
		PeerIDVersions:     "0",
	}
	tlsOptions, err := tlsopts.NewOptions(ident, tlsConfig, nil)
	if err != nil {
		return errs.Wrap(err)
	}

	listenConfig := net.ListenConfig{}

	publicTCPListener, err := listenConfig.Listen(ctx, "tcp", "0.0.0.0:28967")
	if err != nil {
		return errs.Wrap(err)
	}
	publicLMux := drpcmigrate.NewListenMux(publicTCPListener, len(drpcmigrate.DRPCHeader))
	publicTLSDRPCListener := tls.NewListener(publicLMux.Route(drpcmigrate.DRPCHeader), tlsOptions.ServerTLSConfig())

	go publicLMux.Run(ctx)

	mux := drpcmux.New()
	srv := drpcserver.NewWithOptions(
		experiment.NewHandler(
			rpctracing.NewHandler(
				mux,
				jaeger.RemoteTraceHandler),
		),
		drpcserver.Options{
			Manager: rpc.NewDefaultManagerOptions(),
		},
	)

	if err := pb.DRPCRegisterPiecestore(mux, &pieceStore{}); err != nil {
		return errs.Wrap(err)
	}
	err = srv.Serve(ctx, publicTLSDRPCListener)
	if err != nil {
		fmt.Println(err)
	}
	return nil

}

type pieceStore struct {
}

func (p *pieceStore) RetainBig(stream pb.DRPCPiecestore_RetainBigStream) error {
	return nil
}

func (p *pieceStore) Upload(stream pb.DRPCPiecestore_UploadStream) error {
	//TODO implement me
	panic("implement me")
}

func (p *pieceStore) Download(stream pb.DRPCPiecestore_DownloadStream) error {
	recv, err := stream.Recv()
	if err != nil {
		return errs.Wrap(err)
	}
	limit := recv.Limit
	fmt.Println("limit", limit.Limit)
	offset := int64(0)
	for {
		recv, err = stream.Recv()
		if err != nil {
			return errs.Wrap(err)
		}

		out := make([]byte, recv.Order.Amount)
		err = stream.Send(&pb.PieceDownloadResponse{
			Chunk: &pb.PieceDownloadResponse_Chunk{
				Offset: offset,
				Data:   out,
			},
			Hash: &pb.PieceHash{},
		})
		if err != nil {
			return errs.Wrap(err)
		}
		fmt.Println("order fulfilled", recv.Order.Amount)
		offset += recv.Order.Amount
		if offset == limit.Limit {
			break
		}
	}

	return nil
}

func (p *pieceStore) Delete(ctx context.Context, request *pb.PieceDeleteRequest) (*pb.PieceDeleteResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (p *pieceStore) DeletePieces(ctx context.Context, request *pb.DeletePiecesRequest) (*pb.DeletePiecesResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (p *pieceStore) Retain(ctx context.Context, request *pb.RetainRequest) (*pb.RetainResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (p *pieceStore) RestoreTrash(ctx context.Context, request *pb.RestoreTrashRequest) (*pb.RestoreTrashResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (p *pieceStore) Exists(ctx context.Context, request *pb.ExistsRequest) (*pb.ExistsResponse, error) {
	//TODO implement me
	panic("implement me")
}

var _ pb.DRPCPiecestoreServer = &pieceStore{}
