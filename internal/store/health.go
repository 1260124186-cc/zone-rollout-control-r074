package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

// MaxExpiredLeaseScan bounds how many leases the health read inspects when
// counting claims that have passed their deadline but have not been swept
// into the expired state yet. Core entity counts stay exact because Go map
// lengths are O(1); only the time-based lease scan has this cap, and a
// truncated scan is reported through ExpiredLeasesLimited.
const MaxExpiredLeaseScan = 10_000

// CommitResult describes the most recent snapshot commit attempted by this
// process. It is process-local metadata and is never stored in the snapshot.
type CommitResult struct {
	Revision    uint64    `json:"revision"`
	Success     bool      `json:"success"`
	Error       string    `json:"error,omitempty"`
	CommittedAt time.Time `json:"committed_at"`
}

// EntityCounts holds exact core entity totals at the inspection point.
type EntityCounts struct {
	Plans       int `json:"plans"`
	Revisions   int `json:"revisions"`
	Nodes       int `json:"nodes"`
	Leases      int `json:"leases"`
	LeaseGroups int `json:"lease_groups"`
	Probes      int `json:"probes"`
	Assessments int `json:"assessments"`
	AuditEvents int `json:"audit_events"`
}

// StorageAccess reports whether snapshot storage can serve reads and commits.
// The writable probe never creates plans, leases or audit events: it only
// writes, fsyncs and renames a throwaway file next to the snapshot.
type StorageAccess struct {
	Readable           bool   `json:"readable"`
	Writable           bool   `json:"writable"`
	FormatIncompatible bool   `json:"format_incompatible"`
	Detail             string `json:"detail,omitempty"`
}

// HealthInspection is one coherent read of the snapshot. Revision, schema
// metadata, entity counts, the last commit result and expired-claim counting
// are all observed while holding the same read lock, so they describe the
// same committed point even while writes are in flight.
type HealthInspection struct {
	ObservedAt             time.Time     `json:"observed_at"`
	Revision               uint64        `json:"revision"`
	SchemaVersion          int           `json:"schema_version"`
	SupportedSchemaVersion int           `json:"supported_schema_version"`
	Counts                 EntityCounts  `json:"counts"`
	LastCommit             *CommitResult `json:"last_commit"`
	ExpiredActiveLeases    int           `json:"expired_active_leases"`
	ExpiredLeasesLimited   bool          `json:"expired_leases_limited"`
	ExpiredLeaseScanLimit  int           `json:"expired_lease_scan_limit"`
	Storage                StorageAccess `json:"storage"`
}

// InspectHealth gathers snapshot progress, scale, commit and lease-lag
// signals at a single point. now drives the expiry comparison so callers
// stay in control of the reference clock.
func (s *Store) InspectHealth(ctx context.Context, now time.Time) HealthInspection {
	// The directory probe runs outside the state lock so a slow filesystem
	// never blocks business writes, and it leaves no business-side effects.
	probeErr := probeWritable(ctx, s.path)

	s.mu.RLock()
	defer s.mu.RUnlock()

	access := StorageAccess{Readable: true, FormatIncompatible: s.schemaMismatch}
	switch {
	case probeErr != nil:
		access.Writable = false
		access.Detail = probeErr.Error()
	case s.schemaMismatch:
		access.Writable = false
		access.Detail = "snapshot schema version is not supported by this process"
	default:
		access.Writable = true
	}

	report := HealthInspection{
		ObservedAt:             now.UTC(),
		Revision:               s.state.Revision,
		SchemaVersion:          s.state.SchemaVersion,
		SupportedSchemaVersion: SupportedSchemaVersion,
		Counts: EntityCounts{
			Plans:       len(s.state.Plans),
			Revisions:   len(s.state.Revisions),
			Nodes:       len(s.state.Nodes),
			Leases:      len(s.state.Leases),
			LeaseGroups: len(s.state.LeaseGroups),
			Probes:      len(s.state.Probes),
			Assessments: len(s.state.Assessments),
			AuditEvents: len(s.state.AuditEvents),
		},
		ExpiredLeaseScanLimit: MaxExpiredLeaseScan,
		Storage:               access,
	}
	if s.lastCommit != nil {
		frozen := *s.lastCommit
		report.LastCommit = &frozen
	}

	deadline := now.UTC()
	scanned := 0
	for _, lease := range s.state.Leases {
		if scanned == MaxExpiredLeaseScan {
			report.ExpiredLeasesLimited = true
			break
		}
		scanned++
		if lease.State == domain.LeaseClaimed && !lease.ExpiresAt.After(deadline) {
			report.ExpiredActiveLeases++
		}
	}
	return report
}

// probeWritable verifies create/write/fsync/rename permissions in the
// snapshot directory without touching the snapshot itself or any business
// entity. Both probe paths are removed before returning.
func probeWritable(ctx context.Context, snapshotPath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := filepath.Dir(snapshotPath)
	probe, err := os.CreateTemp(dir, ".health-probe-*")
	if err != nil {
		return fmt.Errorf("create write probe: %w", err)
	}
	probePath := probe.Name()
	committedPath := probePath + ".committed"
	defer func() {
		_ = os.Remove(committedPath)
		_ = os.Remove(probePath)
	}()
	if _, err := probe.Write([]byte("zrc-health-probe")); err != nil {
		_ = probe.Close()
		return fmt.Errorf("write probe: %w", err)
	}
	if err := probe.Sync(); err != nil {
		_ = probe.Close()
		return fmt.Errorf("sync probe: %w", err)
	}
	if err := probe.Close(); err != nil {
		return fmt.Errorf("close probe: %w", err)
	}
	if err := os.Rename(probePath, committedPath); err != nil {
		return fmt.Errorf("commit probe rename: %w", err)
	}
	if directory, err := os.Open(dir); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return ctx.Err()
}
