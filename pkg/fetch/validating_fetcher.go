package fetch

import (
	"context"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	"github.com/buildbarn/bb-asset-hub/pkg/qualifier"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type validatingFetcher struct {
	fetcher Fetcher
}

// NewValidatingFetcher creates a fetcher that validates Fetch* requests are valid,
// before passing on to a backend
func NewValidatingFetcher(fetcher Fetcher) Fetcher {
	return &validatingFetcher{
		fetcher: fetcher,
	}
}

func (vf *validatingFetcher) FetchBlob(ctx context.Context, req *remoteasset.FetchBlobRequest) (*remoteasset.FetchBlobResponse, error) {
	if len(req.Uris) == 0 {
		return nil, status.Error(codes.InvalidArgument, "FetchBlob does not support requests without any URIs specified.")
	}
	if unsupported := vf.CheckQualifiers(qualifier.QualifiersToSet(req.Qualifiers)); unsupported != nil {
		return nil, unsupported
	}
	return vf.fetcher.FetchBlob(ctx, req)
}

func (vf *validatingFetcher) FetchDirectory(ctx context.Context, req *remoteasset.FetchDirectoryRequest) (*remoteasset.FetchDirectoryResponse, error) {
	if len(req.Uris) == 0 {
		return nil, status.Error(codes.InvalidArgument, "FetchDirectory does not support requests without any URIs specified.")
	}
	if unsupported := vf.CheckQualifiers(qualifier.QualifiersToSet(req.Qualifiers)); unsupported != nil {
		return nil, unsupported
	}
	return vf.fetcher.FetchDirectory(ctx, req)
}

func (vf *validatingFetcher) CheckQualifiers(qualifiers qualifier.Set) error {
	return vf.fetcher.CheckQualifiers(qualifiers)
}
