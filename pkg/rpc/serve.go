package rpc

import (
	"context"
	"crypto/tls"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
	"net"
	"storj.io/common/identity"
	"storj.io/common/peertls/tlsopts"
	"storj.io/common/rpc"
	"storj.io/drpc/drpcmigrate"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
)

func init() {

	{
		cmd := cobra.Command{
			Use: "serve",
			RunE: func(cmd *cobra.Command, args []string) error {
				return serve(args[0])
			},
		}

		RpcCmd.AddCommand(&cmd)
	}
}

type handler struct {
}

func (h handler) EatCookie(stream DRPCCookieMonster_EatCookieStream) error {
	return nil
}

func serve(nodeId string) error {
	ctx := context.Background()

	conf := tlsopts.Config{}

	idntity, err := identity.Config{
		CertPath: "identity.cert",
		KeyPath:  "identity.key",
	}.Load()
	if err != nil {
		return err
	}

	tlsOptions, err := tlsopts.NewOptions(idntity, conf, nil)
	if err != nil {
		return errs.Wrap(err)
	}

	serverOptions := drpcserver.Options{
		Manager: rpc.NewDefaultManagerOptions(),
	}

	privateListener, err := net.Listen("tcp", "localhost:1443")
	if err != nil {
		return errs.Wrap(err)
	}

	publicMux := drpcmux.New()
	err = DRPCRegisterCookieMonster(publicMux, handler{})
	server := drpcserver.NewWithOptions(publicMux, serverOptions)

	listenMux := drpcmigrate.NewListenMux(privateListener, len(drpcmigrate.DRPCHeader))
	drpcListener := tls.NewListener(listenMux.Route(drpcmigrate.DRPCHeader), tlsOptions.ServerTLSConfig())

	go listenMux.Run(ctx)
	if err != nil {
		return err
	}
	return server.Serve(ctx, drpcListener)

}
