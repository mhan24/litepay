package store

import "errors"

// ClaimRejection is a known business rejection, never a storage/commit failure.
type ClaimRejection struct{ Reason string }

func (e *ClaimRejection) Error() string { return e.Reason }
func rejectClaim(reason string) error   { return &ClaimRejection{Reason: reason} }
func IsClaimRejection(err error) bool {
	var rejection *ClaimRejection
	return errors.As(err, &rejection)
}
