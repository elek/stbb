// Copyright (C) 2020 Storj Labs, Inc.
// See LICENSE for copying information.

package access

import (
	"errors"
	"fmt"
	"io"
	"storj.io/common/errs2"
	"storj.io/common/rpc/rpcstatus"
	"storj.io/uplink"
	"strings"
	"time"
	_ "unsafe" // for go:linkname

	"github.com/zeebo/errs"

	"storj.io/common/encryption"
	"storj.io/common/grant"
	"storj.io/common/macaroon"
	"storj.io/common/rpc"
	"storj.io/common/storj"
	"storj.io/uplink/private/metaclient"
)

// An Access Grant contains everything to access a project and specific buckets.
// It includes a potentially-restricted API Key, a potentially-restricted set
// of encryption information, and information about the Satellite responsible
// for the project's metadata.
type Access struct {
	SatelliteURL storj.NodeURL
	apiKey       *macaroon.APIKey
	encAccess    *grant.EncryptionAccess
}

// getAPIKey are exposing the state do private methods.
//
// NB: this is used with linkname in internal/expose.
// It needs to be updated when this is updated.
//
//lint:ignore U1000, used with linkname
//nolint:unused
//go:linkname access_getAPIKey
func access_getAPIKey(access *Access) *macaroon.APIKey { return access.apiKey }

// getEncAccess are exposing the state do private methods.
//
// NB: this is used with linkname in internal/expose.
// It needs to be updated when this is updated.
//
//lint:ignore U1000, used with linkname
//nolint:unused
//go:linkname access_getEncAccess
func access_getEncAccess(access *Access) *grant.EncryptionAccess { return access.encAccess }

// SharePrefix defines a prefix that will be shared.
type SharePrefix struct {
	Bucket string
	// Prefix is the prefix of the shared object keys.
	//
	// Note: that within a bucket, the hierarchical key derivation scheme is
	// delineated by forward slashes (/), so encryption information will be
	// included in the resulting access grant to decrypt any key that shares
	// the same prefix up until the last slash.
	Prefix string
}

// Permission defines what actions can be used to share.
type Permission struct {
	// AllowDownload gives permission to download the object's content. It
	// allows getting object metadata, but it does not allow listing buckets.
	AllowDownload bool
	// AllowUpload gives permission to create buckets and upload new objects.
	// It does not allow overwriting existing objects unless AllowDelete is
	// granted too.
	AllowUpload bool
	// AllowList gives permission to list buckets. It allows getting object
	// metadata, but it does not allow downloading the object's content.
	AllowList bool
	// AllowDelete gives permission to delete buckets and objects. Unless
	// either AllowDownload or AllowList is granted too, no object metadata and
	// no error info will be returned for deleted objects.
	AllowDelete bool
	// NotBefore restricts when the resulting access grant is valid for.
	// If set, the resulting access grant will not work if the Satellite
	// believes the time is before NotBefore.
	// If set, this value should always be before NotAfter.
	NotBefore time.Time
	// NotAfter restricts when the resulting access grant is valid for.
	// If set, the resulting access grant will not work if the Satellite
	// believes the time is after NotAfter.
	// If set, this value should always be after NotBefore.
	NotAfter time.Time
}

// ParseAccess parses a serialized access grant string.
//
// This should be the main way to instantiate an access grant for opening a project.
// See the note on RequestAccessWithPassphrase.
func ParseAccess(access string) (*Access, error) {
	inner, err := grant.ParseAccess(access)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	satelliteURL, err := parseNodeURL(inner.SatelliteAddress)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	return &Access{
		SatelliteURL: satelliteURL,
		apiKey:       inner.APIKey,
		encAccess:    inner.EncAccess,
	}, nil
}

// SatelliteAddress returns the satellite node URL for this access grant.
func (access *Access) SatelliteAddress() string {
	return access.SatelliteURL.String()
}

// Serialize serializes an access grant such that it can be used later with
// ParseAccess or other tools.
func (access *Access) Serialize() (string, error) {
	inner := grant.Access{
		SatelliteAddress: access.SatelliteURL.String(),
		APIKey:           access.apiKey,
		EncAccess:        access.encAccess,
	}
	return inner.Serialize()
}

// parseNodeURL parses the address into a storj.NodeURL adding the node id if necessary
// for known addresses.
func parseNodeURL(address string) (storj.NodeURL, error) {
	nodeURL, err := storj.ParseNodeURL(address)
	if err != nil {
		return nodeURL, packageError.Wrap(err)
	}

	// Node id is required in satelliteNodeID for all unknown (non-storj) satellites.
	// For known satellite it will be automatically prepended.
	if nodeURL.ID.IsZero() {
		nodeID, found := rpc.KnownNodeID(nodeURL.Address)
		if !found {
			return nodeURL, packageError.New("node id is required in satelliteNodeURL")
		}
		nodeURL.ID = nodeID
	}

	return nodeURL, nil
}

// Share creates a new access grant with specific permissions.
//
// Access grants can only have their existing permissions restricted,
// and the resulting access grant will only allow for the intersection of all previous
// Share calls in the access grant construction chain.
//
// Prefixes, if provided, restrict the access grant (and internal encryption information)
// to only contain enough information to allow access to just those prefixes.
//
// To revoke an access grant see the Project.RevokeAccess method.
func (access *Access) Share(permission Permission, prefixes ...SharePrefix) (*Access, error) {
	internalPrefixes := make([]grant.SharePrefix, 0, len(prefixes))
	for _, prefix := range prefixes {
		internalPrefixes = append(internalPrefixes, grant.SharePrefix(prefix))
	}
	rv, err := access.toInternal().Restrict(grant.Permission(permission), internalPrefixes...)
	if err != nil {
		return nil, err
	}
	return accessFromInternal(rv)
}

func (access *Access) toInternal() *grant.Access {
	return &grant.Access{
		SatelliteAddress: access.SatelliteURL.String(),
		APIKey:           access.apiKey,
		EncAccess:        access.encAccess,
	}
}

func accessFromInternal(access *grant.Access) (*Access, error) {
	satelliteURL, err := parseNodeURL(access.SatelliteAddress)
	if err != nil {
		return nil, packageError.Wrap(err)
	}

	return &Access{
		SatelliteURL: satelliteURL,
		apiKey:       access.APIKey,
		encAccess:    access.EncAccess,
	}, nil
}

// ReadOnlyPermission returns a Permission that allows reading and listing
// (if the parent access grant already allows those things).
func ReadOnlyPermission() Permission {
	return Permission{
		AllowDownload: true,
		AllowList:     true,
	}
}

// WriteOnlyPermission returns a Permission that allows writing and deleting
// (if the parent access grant already allows those things).
func WriteOnlyPermission() Permission {
	return Permission{
		AllowUpload: true,
		AllowDelete: true,
	}
}

// FullPermission returns a Permission that allows all actions that the
// parent access grant already allows.
func FullPermission() Permission {
	return Permission{
		AllowDownload: true,
		AllowUpload:   true,
		AllowList:     true,
		AllowDelete:   true,
	}
}

var packageError = errs.Class("uplink")

//go:linkname convertKnownErrors
func convertKnownErrors(err error, bucket, key string) error {
	switch {
	case errors.Is(err, io.EOF):
		return err
	case metaclient.ErrNoBucket.Has(err):
		return errwrapf("%w (%q)", uplink.ErrBucketNameInvalid, bucket)
	case metaclient.ErrNoPath.Has(err):
		return errwrapf("%w (%q)", uplink.ErrObjectKeyInvalid, key)
	case metaclient.ErrBucketNotFound.Has(err):
		return errwrapf("%w (%q)", uplink.ErrBucketNotFound, bucket)
	case metaclient.ErrObjectNotFound.Has(err):
		return errwrapf("%w (%q)", uplink.ErrObjectNotFound, key)
	case encryption.ErrMissingEncryptionBase.Has(err):
		return errwrapf("%w (%q)", uplink.ErrPermissionDenied, key)
	case encryption.ErrMissingDecryptionBase.Has(err):
		return errwrapf("%w (%q)", uplink.ErrPermissionDenied, key)
	case errs2.IsRPC(err, rpcstatus.ResourceExhausted):
		// TODO is a better way to do this?
		message := errs.Unwrap(err).Error()
		if strings.HasSuffix(message, "Exceeded Usage Limit") {
			return packageError.Wrap(rpcstatus.Wrap(rpcstatus.ResourceExhausted, uplink.ErrBandwidthLimitExceeded))
		} else if strings.HasSuffix(message, "Too Many Requests") {
			return packageError.Wrap(rpcstatus.Wrap(rpcstatus.ResourceExhausted, uplink.ErrTooManyRequests))
		} else if strings.Contains(message, "Exceeded Storage Limit") {
			// contains used to have some flexibility in constructing error message on server-side
			return packageError.Wrap(rpcstatus.Wrap(rpcstatus.ResourceExhausted, uplink.ErrStorageLimitExceeded))
		} else if strings.Contains(message, "Exceeded Segments Limit") {
			// contains used to have some flexibility in constructing error message on server-side
			return packageError.Wrap(rpcstatus.Wrap(rpcstatus.ResourceExhausted, uplink.ErrSegmentsLimitExceeded))
		}
	case errs2.IsRPC(err, rpcstatus.NotFound):
		message := errs.Unwrap(err).Error()
		if strings.HasPrefix(message, metaclient.ErrBucketNotFound.New("").Error()) {
			prefixLength := len(metaclient.ErrBucketNotFound.New("").Error())
			// remove error prefix + ": " from message
			bucket := message[prefixLength+2:]
			return errwrapf("%w (%q)", uplink.ErrBucketNotFound, bucket)
		} else if strings.HasPrefix(message, metaclient.ErrObjectNotFound.New("").Error()) {
			return errwrapf("%w (%q)", uplink.ErrObjectNotFound, key)
		}
	case errs2.IsRPC(err, rpcstatus.PermissionDenied):
		originalErr := err
		wrappedErr := errwrapf("%w (%v)", uplink.ErrPermissionDenied, originalErr)
		// TODO: once we have confirmed nothing downstream
		// is using errs2.IsRPC(err, rpcstatus.PermissionDenied), we should
		// just return wrappedErr instead of this.
		return &joinedErr{main: wrappedErr, alt: originalErr, code: rpcstatus.PermissionDenied}
	}

	return packageError.Wrap(err)
}

func errwrapf(format string, err error, args ...interface{}) error {
	var all []interface{}
	all = append(all, err)
	all = append(all, args...)
	return packageError.Wrap(fmt.Errorf(format, all...))
}

type joinedErr struct {
	main error
	alt  error
	code rpcstatus.StatusCode
}

func (err *joinedErr) Is(target error) bool {
	return errors.Is(err.main, target) || errors.Is(err.alt, target)
}

func (err *joinedErr) As(target interface{}) bool {
	if errors.As(err.main, target) {
		return true
	}
	if errors.As(err.alt, target) {
		return true
	}
	return false
}

func (err *joinedErr) Code() uint64 {
	return uint64(err.code)
}

func (err *joinedErr) Unwrap() error {
	return err.main
}

func (err *joinedErr) Error() string {
	return err.main.Error()
}

// Ungroup works with errs2.IsRPC and errs.IsFunc.
func (err *joinedErr) Ungroup() []error {
	return []error{err.main, err.alt}
}
