package satellite

import (
	"context"
	"crypto/tls"
	_ "embed"
	"fmt"
	"github.com/zeebo/errs/v2"
	"net"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/storj"
	"storj.io/drpc/drpcmigrate"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
	"time"
)

var (
	//go:embed identity.cert
	Certificate []byte

	//go:embed identity.key
	Key []byte
)

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

type HeldAmountEndpoint struct {
	nodeID storj.NodeID
}

func (h HeldAmountEndpoint) GetPayStub(ctx context.Context, request *pb.GetHeldAmountRequest) (*pb.GetHeldAmountResponse, error) {
	return &pb.GetHeldAmountResponse{
		Period:    request.Period,
		CreatedAt: time.Now(),
	}, nil
}

func (h HeldAmountEndpoint) GetAllPaystubs(ctx context.Context, request *pb.GetAllPaystubsRequest) (*pb.GetAllPaystubsResponse, error) {
	return &pb.GetAllPaystubsResponse{
		Paystub: []*pb.GetHeldAmountResponse{},
	}, nil
}

func (h HeldAmountEndpoint) GetPayment(ctx context.Context, request *pb.GetPaymentRequest) (*pb.GetPaymentResponse, error) {
	n := time.Now()
	return &pb.GetPaymentResponse{
		NodeId:    h.nodeID,
		CreatedAt: time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, time.Local),
		Period:    request.Period,
		Amount:    44444,
	}, nil
}

func (h HeldAmountEndpoint) GetAllPayments(ctx context.Context, request *pb.GetAllPaymentsRequest) (*pb.GetAllPaymentsResponse, error) {
	n := time.Now()
	return &pb.GetAllPaymentsResponse{
		Payment: []*pb.GetPaymentResponse{
			{
				NodeId:    h.nodeID,
				CreatedAt: time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, time.Local),
				Period:    time.Date(n.Year(), n.Month()-1, n.Day(), 0, 0, 0, 0, time.Local),
				Amount:    100000000000000,
				Receipt:   "zksync-era:0x85ab6c8f8240a005ef90c1b477e6e61ccf1e6d5463672e0d1c166075ded92c0b",
				Id:        1234,
			},
			{
				NodeId:    h.nodeID,
				CreatedAt: time.Date(n.Year(), n.Month()-1, n.Day(), 0, 0, 0, 0, time.Local),
				Period:    time.Date(n.Year(), n.Month()-2, n.Day(), 0, 0, 0, 0, time.Local),
				Amount:    100000000000000,
				Receipt:   "zkwithdraw:0x85ab6c8f8240a005ef90c1b477e6e61ccf1e6d5463672e0d1c166075ded92c0b",
				Id:        1233,
			},
		},
	}, nil
}

type OrdersEndpoint struct {
}

func (o *OrdersEndpoint) SettlementWithWindow(stream pb.DRPCOrders_SettlementWithWindowStream) error {
	storagenodeSettled := map[int32]int64{}
	for {
		s, err := stream.Recv()
		if err != nil {
			return stream.SendAndClose(&pb.SettlementWithWindowResponse{
				Status:        pb.SettlementWithWindowResponse_ACCEPTED,
				ActionSettled: storagenodeSettled,
			})
		}
		storagenodeSettled[int32(s.Limit.Action)] += s.Order.Amount
	}

}

type Run struct {
}

func (r Run) Run() error {
	ctx := context.Background()
	ident, err := identity.FullIdentityFromPEM(Certificate, Key)
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
	err = pb.DRPCRegisterHeldAmount(m, &HeldAmountEndpoint{})
	if err != nil {
		return errs.Wrap(err)
	}

	err = pb.DRPCRegisterOrders(m, &OrdersEndpoint{})
	if err != nil {
		return errs.Wrap(err)
	}

	serv := drpcserver.NewWithOptions(m, drpcserver.Options{})
	return serv.Serve(ctx, tlsListener)
}
