package uplink

import (
	"bytes"
	"context"
	"io"
	"storj.io/uplink"
)

func Upload(ctx context.Context, cfg uplink.Config, access *uplink.Access, source []byte, bucket string, key string, ) error {
	project, err := cfg.OpenProject(ctx, access)
	if err != nil {
		return err
	}
	defer project.Close()
	
	dest, err := project.UploadObject(ctx, bucket, key, &uplink.UploadOptions{})
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

func Download(ctx context.Context, cfg uplink.Config, access *uplink.Access, bucket string, key string) (res []byte, err error) {
	project, err := cfg.OpenProject(ctx, access)
	if err != nil {
		return
	}
	defer project.Close()

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

func Delete(ctx context.Context, cfg uplink.Config, access *uplink.Access, bucket string, key string) error {
	project, err := cfg.OpenProject(ctx, access)
	if err != nil {
		return err
	}
	defer project.Close()

	_, err = project.DeleteObject(ctx, bucket, key)
	return err

}
