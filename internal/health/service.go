// Package health assembles a coherent point-in-time health view of the
// snapshot store: revision, schema, entity counts, last transaction outcome
// and side-effect-free storage accessibility checks.
package health

import (
	"context"
	"time"

	"solo-0014-zone-rollout-control/internal/store"
)

type Status string

const (
	// StatusOK is readable, writable and uses a supported snapshot format.
	StatusOK Status = "ok"
	// StatusFormatIncompatible means the snapshot was produced by another
	// build: data is still readable but will not be rewritten.
	StatusFormatIncompatible Status = "format_incompatible"
	// StatusReadOnly means the format is supported and the snapshot is
	// readable, but the writability probe failed.
	StatusReadOnly Status = "read_only"
	// StatusDegraded means storage works but durable state does not match
	// the in-memory point in time.
	StatusDegraded Status = "degraded"
	// StatusUnavailable means the snapshot itself could not be read.
	StatusUnavailable Status = "unavailable"
)

type Service struct {
	store *store.Store
}

func New(stateStore *store.Store) *Service {
	return &Service{store: stateStore}
}

// BoundedCount mirrors store.BoundedCount in the service contract so callers
// do not depend on the store package.
type BoundedCount struct {
	Value     int  `json:"value"`
	Capacity  int  `json:"capacity"`
	Truncated bool `json:"truncated"`
}

type Snapshot struct {
	Revision               uint64 `json:"revision"`
	DurableRevision        uint64 `json:"durable_revision,omitempty"`
	SchemaVersion          int    `json:"schema_version"`
	SupportedSchemaVersion int    `json:"supported_schema_version"`
	DurableInSync          bool   `json:"durable_in_sync"`
}

type Counts struct {
	Plans       BoundedCount `json:"plans"`
	Revisions   BoundedCount `json:"revisions"`
	Nodes       BoundedCount `json:"nodes"`
	Leases      BoundedCount `json:"leases"`
	LeaseGroups BoundedCount `json:"lease_groups"`
	Probes      BoundedCount `json:"probes"`
	Assessments BoundedCount `json:"assessments"`
	AuditEvents BoundedCount `json:"audit_events"`
}

type LastCommit struct {
	At                time.Time `json:"at"`
	Success           bool      `json:"success"`
	AttemptedRevision uint64    `json:"attempted_revision"`
	CommittedRevision uint64    `json:"committed_revision,omitempty"`
	ErrorCode         string    `json:"error_code,omitempty"`
	Error             string    `json:"error,omitempty"`
}

type StorageError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Report struct {
	// Status stays first and keeps its historical "ok" value when healthy,
	// so existing clients reading only this field remain compatible.
	Status              Status        `json:"status"`
	CheckedAt           time.Time     `json:"checked_at"`
	Readable            bool          `json:"readable"`
	Writable            bool          `json:"writable"`
	FormatCompatible    bool          `json:"format_compatible"`
	Snapshot            Snapshot      `json:"snapshot"`
	Counts              Counts        `json:"counts"`
	ExpiredActiveLeases BoundedCount  `json:"expired_active_leases"`
	Truncated           bool          `json:"truncated"`
	LastCommit          *LastCommit   `json:"last_commit,omitempty"`
	StorageError        *StorageError `json:"storage_error,omitempty"`
}

func (s *Service) Inspect(ctx context.Context) (Report, error) {
	inspection, err := s.store.InspectHealth(ctx)
	if err != nil {
		return Report{}, err
	}
	report := Report{
		CheckedAt: inspection.CheckedAt,
		Readable:  inspection.Storage.Readable,
		Writable:  inspection.Storage.Writable,
		FormatCompatible: !inspection.IncompatibleSchema &&
			inspection.Storage.DurableFormatOK,
		Snapshot: Snapshot{
			Revision:               inspection.Revision,
			DurableRevision:        inspection.Storage.DurableRevision,
			SchemaVersion:          inspection.SchemaVersion,
			SupportedSchemaVersion: inspection.SupportedSchema,
			DurableInSync:          inspection.Storage.DurableInSync,
		},
		Counts: Counts{
			Plans:       convertCount(inspection.Counts.Plans),
			Revisions:   convertCount(inspection.Counts.Revisions),
			Nodes:       convertCount(inspection.Counts.Nodes),
			Leases:      convertCount(inspection.Counts.Leases),
			LeaseGroups: convertCount(inspection.Counts.LeaseGroups),
			Probes:      convertCount(inspection.Counts.Probes),
			Assessments: convertCount(inspection.Counts.Assessments),
			AuditEvents: convertCount(inspection.Counts.AuditEvents),
		},
		ExpiredActiveLeases: convertCount(inspection.ExpiredActiveLeases),
	}
	if inspection.LastCommit != nil {
		report.LastCommit = &LastCommit{
			At:                inspection.LastCommit.At,
			Success:           inspection.LastCommit.Success,
			AttemptedRevision: inspection.LastCommit.AttemptedRev,
			CommittedRevision: inspection.LastCommit.CommittedRev,
			ErrorCode:         inspection.LastCommit.ErrorCode,
			Error:             inspection.LastCommit.Error,
		}
	}
	if inspection.Storage.ErrorCode != "" {
		report.StorageError = &StorageError{
			Code:    inspection.Storage.ErrorCode,
			Message: inspection.Storage.Error,
		}
	}
	report.Truncated = anyTruncated(report.Counts, report.ExpiredActiveLeases)
	report.Status = rollupStatus(report)
	return report, nil
}

func rollupStatus(report Report) Status {
	switch {
	case !report.Readable:
		return StatusUnavailable
	case !report.FormatCompatible:
		return StatusFormatIncompatible
	case !report.Snapshot.DurableInSync:
		return StatusDegraded
	case !report.Writable:
		return StatusReadOnly
	default:
		return StatusOK
	}
}

func anyTruncated(counts Counts, expired BoundedCount) bool {
	return counts.Plans.Truncated ||
		counts.Revisions.Truncated ||
		counts.Nodes.Truncated ||
		counts.Leases.Truncated ||
		counts.LeaseGroups.Truncated ||
		counts.Probes.Truncated ||
		counts.Assessments.Truncated ||
		counts.AuditEvents.Truncated ||
		expired.Truncated
}

func convertCount(value store.BoundedCount) BoundedCount {
	return BoundedCount{
		Value:     value.Value,
		Capacity:  value.Capacity,
		Truncated: value.Truncated,
	}
}
