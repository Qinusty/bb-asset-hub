package fetch_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-asset-hub/internal/mock"
	"github.com/buildbarn/bb-asset-hub/pkg/fetch"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bb_digest "github.com/buildbarn/bb-storage/pkg/digest"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFetchBlobCompletenessChecking(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	instanceName, err := bb_digest.NewInstanceName("")
	require.NoError(t, err)

	uri := "www.example.com"
	request := &remoteasset.FetchBlobRequest{
		InstanceName: "",
		Uris:         []string{uri},
	}
	blobDigest := &remoteexecution.Digest{Hash: "d0d829c4c0ce64787cb1c998a9c29a109f8ed005633132fda4f29982487b04db", SizeBytes: 123}

	backend := mock.NewMockBlobAccess(ctrl)
	mockFetcher := mock.NewMockFetchServer(ctrl)
	ccFetcher := fetch.NewCompletenessCheckingFetcher(mockFetcher, backend, 5, 16*1024*1024)

	t.Run("Success", func(t *testing.T) {
		fetchBlobCall := mockFetcher.EXPECT().FetchBlob(ctx, request).Return(&remoteasset.FetchBlobResponse{
			Status:     status.New(codes.OK, "Success!").Proto(),
			Uri:        uri,
			BlobDigest: blobDigest,
		}, nil)
		backend.EXPECT().FindMissing(ctx, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digests digest.Set) (digest.Set, error) {
				firstDigest, ok := digests.First()
				require.True(t, ok)
				digestProto := firstDigest.GetProto()
				require.True(t, proto.Equal(blobDigest, digestProto))
				return digest.EmptySet, nil
			}).After(fetchBlobCall)
		response, err := ccFetcher.FetchBlob(ctx, request)
		require.Nil(t, err)
		require.Equal(t, response.Status.Code, int32(codes.OK))
	})

	t.Run("FailureBlobNotInCas", func(t *testing.T) {
		fetchBlobCall := mockFetcher.EXPECT().FetchBlob(ctx, request).Return(&remoteasset.FetchBlobResponse{
			Status:     status.New(codes.OK, "Success!").Proto(),
			Uri:        uri,
			BlobDigest: blobDigest,
		}, nil)
		backend.EXPECT().FindMissing(ctx, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digests digest.Set) (digest.Set, error) {
				firstDigest, ok := digests.First()
				require.True(t, ok)
				digestProto := firstDigest.GetProto()
				require.True(t, proto.Equal(blobDigest, digestProto))
				bbDigest, _ := instanceName.NewDigestFromProto(digestProto)
				return digest.NewSetBuilder().Add(bbDigest).Build(), nil
			}).After(fetchBlobCall)
		response, err := ccFetcher.FetchBlob(ctx, request)
		require.NotNil(t, err)
		require.Nil(t, response)
		require.Equal(t, status.Code(err), codes.NotFound)
	})

	t.Run("UnderlyingFetcherFailure", func(t *testing.T) {
		mockFetcher.EXPECT().FetchBlob(ctx, request).Return(nil, status.Error(codes.NotFound, "Blob not found!"))
		response, err := ccFetcher.FetchBlob(ctx, request)
		require.NotNil(t, err)
		require.Nil(t, response)
		require.Equal(t, status.Code(err), codes.NotFound)
	})
}

func TestFetchDirectoryCompletenessChecking(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	instanceName, err := bb_digest.NewInstanceName("")
	require.NoError(t, err)

	uri := "www.example.com"
	request := &remoteasset.FetchDirectoryRequest{
		InstanceName: "",
		Uris:         []string{uri},
	}
	treeDigest := &remoteexecution.Digest{Hash: "d0d829c4c0ce64787cb1c998a9c29a109f8ed005633132fda4f29982487b04db", SizeBytes: 123}
	bbTreeDigest, _ := instanceName.NewDigestFromProto(treeDigest)
	treeProto := &remoteexecution.Tree{
		Root: &remoteexecution.Directory{
			// Directory digests should not be part of
			// FindMissing(), as references to directories
			// are contained within the Tree object itself.
			Directories: []*remoteexecution.DirectoryNode{
				{
					Digest: &remoteexecution.Digest{
						Hash:      "7a3435d88e819881cbe9d430a340d157",
						SizeBytes: 10,
					},
				},
			},
			Files: []*remoteexecution.FileNode{
				{
					Digest: &remoteexecution.Digest{
						Hash:      "eda14e187a768b38eda999457c9cca1e",
						SizeBytes: 6,
					},
				},
			},
		},
		Children: []*remoteexecution.Directory{
			{
				Files: []*remoteexecution.FileNode{
					{
						Digest: &remoteexecution.Digest{
							Hash:      "6c396013ff0ebff6a2a96cdc20a4ba4c",
							SizeBytes: 5,
						},
					},
				},
			},
			{},
		},
	}

	backend := mock.NewMockBlobAccess(ctrl)
	mockFetcher := mock.NewMockFetchServer(ctrl)
	ccFetcher := fetch.NewCompletenessCheckingFetcher(mockFetcher, backend, 5, 16*1024*1024)

	t.Run("Success", func(t *testing.T) {
		fetchDirectoryCall := mockFetcher.EXPECT().FetchDirectory(ctx, request).Return(&remoteasset.FetchDirectoryResponse{
			Status:              status.New(codes.OK, "Success!").Proto(),
			Uri:                 uri,
			RootDirectoryDigest: treeDigest,
		}, nil)

		getDirCall := backend.EXPECT().Get(ctx, bbTreeDigest).Return(
			buffer.NewProtoBufferFromProto(treeProto, buffer.Irreparable)).After(fetchDirectoryCall)
		backend.EXPECT().FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("", "6c396013ff0ebff6a2a96cdc20a4ba4c", 5)).
				Add(digest.MustNewDigest("", "eda14e187a768b38eda999457c9cca1e", 6)).
				Build(),
		).Return(digest.EmptySet, nil).After(getDirCall)
		response, err := ccFetcher.FetchDirectory(ctx, request)
		require.Nil(t, err)
		require.Equal(t, response.Status.Code, int32(codes.OK))
	})

	t.Run("MissingRoot", func(t *testing.T) {
		fetchDirectoryCall := mockFetcher.EXPECT().FetchDirectory(ctx, request).Return(&remoteasset.FetchDirectoryResponse{
			Status:              status.New(codes.OK, "Success!").Proto(),
			Uri:                 uri,
			RootDirectoryDigest: treeDigest,
		}, nil)

		backend.EXPECT().Get(ctx, bbTreeDigest).Return(
			buffer.NewBufferFromError(status.Error(codes.NotFound, "Not Found!"))).After(fetchDirectoryCall)
		_, err := ccFetcher.FetchDirectory(ctx, request)
		require.NotNil(t, err)
		require.Equal(t, status.Code(err), codes.NotFound)
	})

	t.Run("MissingFile", func(t *testing.T) {
		fetchDirectoryCall := mockFetcher.EXPECT().FetchDirectory(ctx, request).Return(&remoteasset.FetchDirectoryResponse{
			Status:              status.New(codes.OK, "Success!").Proto(),
			Uri:                 uri,
			RootDirectoryDigest: treeDigest,
		}, nil)

		getDirCall := backend.EXPECT().Get(ctx, bbTreeDigest).Return(
			buffer.NewProtoBufferFromProto(treeProto, buffer.Irreparable)).After(fetchDirectoryCall)
		backend.EXPECT().FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("", "6c396013ff0ebff6a2a96cdc20a4ba4c", 5)).
				Add(digest.MustNewDigest("", "eda14e187a768b38eda999457c9cca1e", 6)).
				Build(),
		).Return(digest.NewSetBuilder().Add(digest.MustNewDigest("", "6c396013ff0ebff6a2a96cdc20a4ba4c", 5)).Build(), nil).After(getDirCall)
		_, err := ccFetcher.FetchDirectory(ctx, request)
		require.NotNil(t, err)
		require.Equal(t, status.Code(err), codes.NotFound)
	})

	t.Run("UnderlyingFetcherFailure", func(t *testing.T) {
		mockFetcher.EXPECT().FetchDirectory(ctx, request).Return(nil, status.Error(codes.NotFound, "Directory not found!"))

		_, err := ccFetcher.FetchDirectory(ctx, request)
		require.NotNil(t, err)
		require.Equal(t, status.Code(err), codes.NotFound)
	})
}
