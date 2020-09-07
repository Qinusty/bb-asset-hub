package qualifier

import (
	"fmt"

	remoteasset "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var exists = struct{}{}

// Set implements a HashSet of qualifier names
type Set map[string]struct{}

// NewSet creates a new Set from a list of qualifier names
func NewSet(names []string) Set {
	s := Set{}
	for _, n := range names {
		s[n] = exists
	}
	return s
}

// IsEmpty checks if the Set is empty
func (s Set) IsEmpty() bool {
	return len(s) == 0
}

// Contains checks if the set contains a given Qualifier Name
func (s Set) Contains(q string) bool {
	_, ok := s[q]
	return ok
}

// Add adds a qualifier name to the set
func (s Set) Add(q string) {
	s[q] = exists
}

// Difference calculates the Set difference a \ b.
func Difference(a Set, b Set) Set {
	diff := Set{}
	for k := range a {
		if !b.Contains(k) {
			diff.Add(k)
		}
	}
	return diff
}

// QualifiersToSet converts an array of qualifiers into a Set of names
func QualifiersToSet(qualifiers []*remoteasset.Qualifier) Set {
	s := Set{}
	for _, q := range qualifiers {
		s.Add(q.Name)
	}
	return s
}

// UnsupportedSetToError converts a set of qualifier names into an RPC error with
// details of each unsupported error.
func UnsupportedSetToError(s Set) error {
	if s.IsEmpty() {
		return nil
	}
	violations := []*errdetails.BadRequest_FieldViolation{}
	for q := range s {
		violations = append(violations, &errdetails.BadRequest_FieldViolation{
			Field:       "qualifiers.name",
			Description: fmt.Sprintf("\"%s\" not supported", q),
		})
	}
	ret, err := status.New(codes.InvalidArgument, "Unsupported Qualifier(s) found in request.").WithDetails(
		&errdetails.BadRequest{
			FieldViolations: violations,
		})
	if err != nil {
		return err
	}
	return ret.Err()
}
