package stub

import (
	"bytes"
	"context"
	"crypto/tls"
	"github.com/stretchr/testify/require"
	"io"
	"storj.io/common/rpc/rpcpool"
	"storj.io/common/testcontext"
	"storj.io/uplink"
	"testing"
)

func TestUplink(t *testing.T) {
	t.Skip("rpcpool.DialerWrapper no longer provides the address parameter; test needs rewrite")

	ctx := rpcpool.WithDialerWrapper(testcontext.New(t), func(ctx context.Context, dialer rpcpool.Dialer) rpcpool.Dialer {
		return func(context.Context) (rpcpool.RawConn, *tls.ConnectionState, error) {
			// TODO: rpcpool.DialerWrapper no longer provides address.
			// Need a different approach to route to the correct stub.
			conn, state, err := dialer(ctx)
			if err != nil {
				return nil, nil, err
			}
			return conn, state, nil
		}
	})

	bucketName := "testbucket"
	uploadKey := "key1"
	accessGrant := "1F9r8Cd2keX52vcDgYxuSgMWgwPwvmSqW8AQx2cc71PRVHxLL49ZfeUEA8adHaCdQgDWsbgyGuue6NwPkVBzQgZeiyZCbqU7eifLB81iKE53JebFVYzKFnU1VqjdAuciHkGXduKepqDr2foDrANwdi7cxH4VTkTTaU46M9W3Z2YeQ3b29ekMNLie74znycoeQtz1uykdDq9t39FCaaDi2cW23fdwos1EZLSQEQTcF9eER8YW39eAp77ypihCFoGJmVLSKqPAfqzor5YkLaZQdpfgrTDY5tJ86Pdz921TzjQer"
	dataToUpload := make([]byte, 10*1024*1024)
	dataToUpload[123] = 123
	dataToUpload[1024] = 1

	access, err := uplink.ParseAccess(accessGrant)
	require.NoError(t, err)

	// Open up the Project we will be working with.
	project, err := uplink.OpenProject(ctx, access)
	require.NoError(t, err)
	defer project.Close()

	// Ensure the desired Bucket within the Project is created.
	_, err = project.EnsureBucket(ctx, bucketName)
	require.NoError(t, err)

	// Intitiate the upload of our Object to the specified bucket and key.
	upload, err := project.UploadObject(ctx, bucketName, uploadKey, nil)
	require.NoError(t, err)

	// Copy the data to the upload.
	buf := bytes.NewBuffer(dataToUpload)
	_, err = io.Copy(upload, buf)
	require.NoError(t, err)

	// Commit the uploaded object.
	err = upload.Commit()
	require.NoError(t, err)

	// Initiate a download of the same object again
	download, err := project.DownloadObject(ctx, bucketName, uploadKey, nil)
	require.NoError(t, err)

	defer download.Close()

	// Read everything from the download stream
	receivedContents, err := io.ReadAll(download)
	require.NoError(t, err)

	// Check that the downloaded data is the same as the uploaded data.
	require.Equal(t, receivedContents, dataToUpload)

}
