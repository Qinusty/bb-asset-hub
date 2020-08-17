package push

import (
	"context"
	"log"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	"google.golang.org/grpc/status"
)

type loggingPusher struct {
	pusher remoteasset.PushServer
}

// NewLoggingPusher creates a wrapper around a Push Server to log requests and responses
func NewLoggingPusher(pusher remoteasset.PushServer) remoteasset.PushServer {
	return &loggingPusher{
		pusher: pusher,
	}
}

func (lp *loggingPusher) PushBlob(ctx context.Context, req *remoteasset.PushBlobRequest) (*remoteasset.PushBlobResponse, error) {
	log.Printf("Pushing Blob %s with qualifiers %s", req.Uris, req.Qualifiers)
	resp, err := lp.pusher.PushBlob(ctx, req)
	log.Printf("PushBlob completed for %s with staus code %d", req.Uris, status.Code(err))
	return resp, err
}

func (lp *loggingPusher) PushDirectory(ctx context.Context, req *remoteasset.PushDirectoryRequest) (*remoteasset.PushDirectoryResponse, error) {
	log.Printf("Pushing Directory %s with qualifiers %s", req.Uris, req.Qualifiers)
	resp, err := lp.pusher.PushDirectory(ctx, req)
	log.Printf("PushDirectory completed for %s with staus code %d", req.Uris, status.Code(err))
	return resp, err
}
