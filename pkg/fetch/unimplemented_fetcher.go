package fetch

import (
	"context"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type unimplementedFetcher struct {
}

// NewUnimplementedFetcher creates a blank Fetcher with both FetchBlob
// and FetchDirectory unimplemented
func NewUnimplementedFetcher() remoteasset.FetchServer {
	return &unimplementedFetcher{}
}

func (f *unimplementedFetcher) FetchBlob(ctx context.Context, req *remoteasset.FetchBlobRequest) (*remoteasset.FetchBlobResponse, error) {
	return &remoteasset.FetchBlobResponse{
		Status: status.New(codes.Unimplemented, "FetchBlob not implemented").Proto(),
	}, nil
}

func (f *unimplementedFetcher) FetchDirectory(ctx context.Context, req *remoteasset.FetchDirectoryRequest) (*remoteasset.FetchDirectoryResponse, error) {
	return &remoteasset.FetchDirectoryResponse{
		Status: status.New(codes.Unimplemented, "FetchDirectory not implemented").Proto(),
	}, nil
}
