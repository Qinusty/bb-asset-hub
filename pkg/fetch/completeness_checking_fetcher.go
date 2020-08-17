package fetch

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/completenesschecking"
	bb_digest "github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	// protostatus "google.golang.org/genproto/googleapis/rpc/status"
	// "google.golang.org/grpc/status"
)

type completenessCheckingFetcher struct {
	fetcher                   remoteasset.FetchServer
	contentAddressableStorage blobstore.BlobAccess
	batchSize                 int
	maximumMessageSizeBytes   int
}

// NewErrorFetcher creates a Remote Asset API Fetch service which simply returns a
// set gRPC status
func NewCompletenessCheckingFetcher(fetcher remoteasset.FetchServer, contentAddressableStorage blobstore.BlobAccess,
	batchSize int, maximumMessageSizeBytes int) remoteasset.FetchServer {
	return &completenessCheckingFetcher{
		fetcher:                   fetcher,
		contentAddressableStorage: contentAddressableStorage,
		batchSize:                 batchSize,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
	}
}

func (cf *completenessCheckingFetcher) FetchBlob(ctx context.Context, req *remoteasset.FetchBlobRequest) (*remoteasset.FetchBlobResponse, error) {
	response, err := cf.fetcher.FetchBlob(ctx, req)
	if err != nil {
		return nil, err
	}
	instanceName, err := bb_digest.NewInstanceName(req.InstanceName)
	findMissingQueue := completenesschecking.NewFindMissingQueue(ctx, instanceName, cf.contentAddressableStorage, cf.batchSize)

	if err := findMissingQueue.Add(response.BlobDigest); err != nil {
		// TODO: Delete asset reference and retry cf.fetcher.FetchBlob()
		return nil, util.StatusWrapf(err, "Failed completeness check whilst fetching blob %s", response.Uri)
	}
	if err := findMissingQueue.Finalize(); err != nil {
		// TODO: Delete asset reference and retry cf.fetcher.FetchBlob()
		return nil, util.StatusWrapf(err, "Failed completeness check whilst fetching blob %s", response.Uri)
	}

	return response, err
}

func (cf *completenessCheckingFetcher) FetchDirectory(ctx context.Context, req *remoteasset.FetchDirectoryRequest) (*remoteasset.FetchDirectoryResponse, error) {
	response, err := cf.fetcher.FetchDirectory(ctx, req)
	if err != nil {
		return nil, err
	}

	instanceName, err := bb_digest.NewInstanceName(req.InstanceName)
	if err != nil {
		return nil, err
	}

	if err := cf.checkDirectoryCompleteness(ctx, instanceName, response.RootDirectoryDigest); err != nil {
		// TODO: Handle failed completeness?
		return nil, util.StatusWrapf(err, "Failed completeness check whilst fetching directory %s", response.Uri)
	}

	return response, err
}

// Fetch the tree associated with the root digest and
// Iterate over all remoteexecution.Digest fields below the root
// directory (remoteexecution.Tree objects)
// referenced by the ActionResult.
func (cf *completenessCheckingFetcher) checkDirectoryCompleteness(ctx context.Context, instanceName bb_digest.InstanceName,
	rootDigest *remoteexecution.Digest) error {
	findMissingQueue := completenesschecking.NewFindMissingQueue(ctx, instanceName, cf.contentAddressableStorage, cf.batchSize)

	treeDigest, err := findMissingQueue.DeriveDigest(rootDigest)
	if err != nil {
		return err
	}
	treeMessage, err := cf.contentAddressableStorage.Get(ctx, treeDigest).ToProto(&remoteexecution.Tree{}, cf.maximumMessageSizeBytes)
	if err != nil {
		return util.StatusWrapf(err, "Referenced Directory Tree %s is not present in the Content Addressable Storage", treeDigest)
	}
	tree := treeMessage.(*remoteexecution.Tree)
	if err := findMissingQueue.AddDirectory(tree.Root); err != nil {
		return err
	}
	for _, child := range tree.Children {
		if err := findMissingQueue.AddDirectory(child); err != nil {
			return err
		}
	}
	return findMissingQueue.Finalize()
}
