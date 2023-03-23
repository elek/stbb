package uplink

import (
	"context"
	"github.com/elek/stbb/pkg/util"
	"github.com/zeebo/errs"
	"io"
	"os"
	"storj.io/storj/cmd/uplink/ulloc"
	"storj.io/uplink"
	"storj.io/uplink/private/testuplink"
	//	"storj.io/uplink/private/testuplink"
)

type Upload struct {
	util.Loop
	Refactored bool   `help:"Use code from upload code refactor"`
	Source     string `arg:"" name:"source"`
	Target     string `arg:"" name:"target"`
}

func (u *Upload) Run() error {
	ctx := context.Background()

	if u.Refactored {
		ctx = testuplink.WithConcurrentSegmentUploadsDefaultConfig(ctx)
	}
	gr := os.Getenv("UPLINK_ACCESS")

	access, err := uplink.ParseAccess(gr)
	if err != nil {
		return err
	}

	p, err := ulloc.Parse(u.Target)
	if err != nil {
		return err
	}

	bucket, key, ok := p.RemoteParts()
	if !ok {
		return errs.New("Path is not remote %s", u.Target)
	}

	cfg := uplink.Config{
		UserAgent: "stbb",
	}

	_, err = u.Loop.Run(func() error {
		return uploadOne(ctx, cfg, access, u.Source, bucket, key)
	})
	return err
}

func uploadOne(ctx context.Context, cfg uplink.Config, access *uplink.Access, from, bucket string, key string) error {
	project, err := cfg.OpenProject(ctx, access)
	if err != nil {
		return err
	}
	defer project.Close()

	source, err := os.Open(from)
	if err != nil {
		return err
	}

	dest, err := project.UploadObject(ctx, bucket, key, &uplink.UploadOptions{})
	if err != nil {
		return err
	}

	_, err = io.Copy(dest, source)
	if err != nil {
		return err
	}

	err = dest.Commit()
	if err != nil {
		return err
	}
	return nil
}
