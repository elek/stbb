package satellite

import (
	"context"
	"crypto/tls"
	_ "embed"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
	"net"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/peertls/tlsopts"
	"storj.io/drpc/drpcmigrate"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
	"time"
)

var (
	//go:embed identity.cert
	cert []byte

	//go:embed identity.key
	key []byte
)

func init() {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run mock satellite",
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return run()
	}
	SatelliteCmd.AddCommand(cmd)
}

type NodeEndpoint struct {
	pb.DRPCNodeUnimplementedServer
}

func (s *NodeEndpoint) GetTime(context.Context, *pb.GetTimeRequest) (*pb.GetTimeResponse, error) {
	return &pb.GetTimeResponse{
		Timestamp: time.Now(),
	}, nil
}

func (s *NodeEndpoint) CheckIn(context.Context, *pb.CheckInRequest) (*pb.CheckInResponse, error) {
	return &pb.CheckInResponse{
		PingNodeSuccess: true,
	}, nil
}

type NodeStatEndpoint struct {
}

func (n *NodeStatEndpoint) DailyStorageUsage(ctx context.Context, request *pb.DailyStorageUsageRequest) (*pb.DailyStorageUsageResponse, error) {
	return &pb.DailyStorageUsageResponse{}, nil
}

func (n *NodeStatEndpoint) PricingModel(ctx context.Context, request *pb.PricingModelRequest) (*pb.PricingModelResponse, error) {
	return &pb.PricingModelResponse{
		EgressBandwidthPrice: 1,
	}, nil
}

type OrderEndpoint struct {
	pb.DRPCOrdersUnimplementedServer
}

func (s *OrderEndpoint) SettlementWithWindow(stream pb.DRPCOrders_SettlementWithWindowStream) error {
	for {
		_, err := stream.Recv()
		if err != nil {
			return err
		}
	}
}

func (n *NodeStatEndpoint) GetStats(context.Context, *pb.GetStatsRequest) (*pb.GetStatsResponse, error) {
	return &pb.GetStatsResponse{
		UptimeCheck: &pb.ReputationStats{
			ReputationScore: 1,
		},
		AuditCheck: &pb.ReputationStats{
			ReputationScore: 1,
		},
	}, nil
}

func run() error {
	ctx := context.Background()
	ident, err := identity.FullIdentityFromPEM(cert, key)
	fmt.Print("Starting ", ident.ID.String()+"@localhost:5656")
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

	tcpListener, err := net.Listen("tcp", "0.0.0.0:5656")
	if err != nil {
		return errs.Wrap(err)
	}
	listenMux := drpcmigrate.NewListenMux(tcpListener, len(drpcmigrate.DRPCHeader))
	tlsListener := tls.NewListener(listenMux.Route(drpcmigrate.DRPCHeader), tlsOptions.ServerTLSConfig())
	go listenMux.Run(ctx)
	m := drpcmux.New()

	err = pb.DRPCRegisterNode(m, &NodeEndpoint{})
	if err != nil {
		return errs.Wrap(err)
	}
	err = pb.DRPCRegisterNodeStats(m, &NodeStatEndpoint{})
	if err != nil {
		return errs.Wrap(err)
	}
	err = pb.DRPCRegisterOrders(m, &OrderEndpoint{})

	serv := drpcserver.NewWithOptions(m, drpcserver.Options{})
	return serv.Serve(ctx, tlsListener)
}
