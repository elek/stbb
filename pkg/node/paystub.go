package node

import (
	"context"
	"fmt"
	"github.com/elek/stbb/pkg/satellite"
	"github.com/elek/stbb/pkg/util"
	"github.com/pkg/errors"
	"os"
	"storj.io/common/identity"
	"storj.io/common/pb"
	"storj.io/common/storj"
	"storj.io/storj/private/date"
)

type Paystub struct {
	URL    string `arg:""`
	Period string `arg:""`
	util.DialerHelper
}

//
//func (p Paystub) Run() error {
//	ctx := context.Background()
//
//	satellite.Certificate, _ = os.ReadFile("identity.cert")
//	satellite.Key, _ = os.ReadFile("identity.key")
//	ident, err := identity.FullIdentityFromPEM(satellite.Certificate, satellite.Key)
//	if err != nil {
//		return err
//	}
//
//	dialer, err := util.GetDialerForIdentity(ctx, ident, false, false)
//	if err != nil {
//		return err
//	}
//	nodeURL, err := storj.ParseNodeURL(p.URL)
//	if err != nil {
//		return err
//	}
//	logger, err := zap.NewDevelopment()
//	if err != nil {
//		return err
//	}
//	ps := payouts.NewEndpoint(logger, dialer, nil)
//	payment, err := ps.GetPayment(ctx, nodeURL.ID, "2023-09")
//	if err != nil {
//		return err
//	}
//	fmt.Println(payment)
//	return nil
//}

func (g Paystub) Run() error {
	ctx := context.Background()

	satellite.Certificate, _ = os.ReadFile("identity.cert")
	satellite.Key, _ = os.ReadFile("identity.key")
	ident, err := identity.FullIdentityFromPEM(satellite.Certificate, satellite.Key)
	if err != nil {
		return err
	}

	dialer, err := util.GetDialerForIdentity(ctx, ident, false, false)

	if err != nil {
		return errors.WithStack(err)
	}

	nodeURL, err := storj.ParseNodeURL(g.URL)
	if err != nil {
		return errors.WithStack(err)
	}

	conn, err := dialer.DialNodeURL(ctx, nodeURL)
	if err != nil {
		return errors.WithStack(err)
	}

	client := pb.NewDRPCHeldAmountClient(conn)

	period, err := date.PeriodToTime(g.Period)
	if err != nil {
		return errors.WithStack(err)
	}

	model, err := client.GetPayStub(ctx, &pb.GetHeldAmountRequest{
		Period: period,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	fmt.Println(model)

	return nil
}
