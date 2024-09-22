package load

import (
	"bytes"
	"context"
	"github.com/spacemonkeygo/monkit/v3"
	"io"
	"storj.io/uplink"
	"time"
)

var mon = monkit.Package()

func Upload(ctx context.Context, project *uplink.Project, source []byte, bucket string, key string, duration time.Duration) (err error) {
	mon.Task()(&ctx)(&err)
	opts := uplink.UploadOptions{}
	if duration > 0 {
		opts.Expires = time.Now().Add(duration)
	}
	dest, err := project.UploadObject(ctx, bucket, key, &opts)
	if err != nil {
		return err
	}

	_, err = dest.Write(source)
	if err != nil {
		return err
	}

	err = dest.Commit()
	if err != nil {
		return err
	}
	return nil
}

func Download(ctx context.Context, project *uplink.Project, bucket string, key string) (res []byte, err error) {
	mon.Task()(&ctx)(&err)
	source, err := project.DownloadObject(ctx, bucket, key, nil)
	if err != nil {
		return
	}
	defer source.Close()

	dest := bytes.NewBuffer([]byte{})

	_, err = io.Copy(dest, source)
	if err != nil {
		return
	}

	return dest.Bytes(), nil
}

func Delete(ctx context.Context, project *uplink.Project, bucket string, key string) (err error) {
	mon.Task()(&ctx)(&err)
	_, err = project.DeleteObject(ctx, bucket, key)
	return err

}
