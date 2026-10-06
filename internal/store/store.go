package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
)

// HealthInspectionCapacity bounds every counter reported by a health
// inspection. When a collection reaches the bound its count is reported as
// the bound itself and truncated is set, so health reads stay constant-work
// and their payload size remains explicit even for very large snapshots.
const HealthInspectionCapacity = 1_000_000

// CommitOutcome describes the most recent store transaction attempt.
type CommitOutcome struct {
	At           time.Time
	Success      bool
	AttemptedRev uint64
	CommittedRev uint64
	ErrorCode    string
	Error        string
}

// HealthInspection is a coherent, single-point-in-time view of the store.
type HealthInspection struct {
	CheckedAt           time.Time
	Revision            uint64
	SchemaVersion       int
	SupportedSchema     int
	IncompatibleSchema  bool
	Counts              HealthCounts
	ExpiredActiveLeases BoundedCount
	Storage             StorageInspection
	LastCommit          *CommitOutcome
}

// HealthCounts holds the core entity totals captured at the inspection point.
type HealthCounts struct {
	Plans       BoundedCount
	Revisions   BoundedCount
	Nodes       BoundedCount
	Leases      BoundedCount
	LeaseGroups BoundedCount
	Probes      BoundedCount
	Assessments BoundedCount
	AuditEvents BoundedCount
}

// BoundedCount is a count with an explicit capacity boundary.
type BoundedCount struct {
	Value     int  `json:"value"`
	Capacity  int  `json:"capacity"`
	Truncated bool `json:"truncated"`
}

// StorageInspection reports side-effect-free storage checks performed at the
// same point in time as the rest of the inspection.
type StorageInspection struct {
	Readable             bool
	Writable             bool
	DurableFormatOK      bool
	DurableRevision      uint64
	DurableSchemaVersion int
	DurableInSync        bool
	ErrorCode            string
	Error                string
}

type Store struct {
	mu                 sync.RWMutex
	path               string
	clock              platform.Clock
	state              domain.State
	incompatibleSchema bool
	lastCommit         *CommitOutcome
}

func Open(path string, clock platform.Clock) (*Store, error) {
	if path == "" {
		return nil, errors.New("snapshot path is required")
	}
	state := blankState()
	payload, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(payload, &state); err != nil {
			return nil, fmt.Errorf("decode snapshot: %w", err)
		}
		// Preserve the decoded schema version (normalizeReadOnly must not
		// default it) so a snapshot written by a newer build is kept
		// read-only instead of being silently rewritten by this build.
		decodedSchema := state.SchemaVersion
		incompatible := decodedSchema != 0 && decodedSchema != schemaVersion
		state = normalizeReadOnly(state)
		state.SchemaVersion = decodedSchema
		return &Store{
			path:               path,
			clock:              clock,
			state:              state,
			incompatibleSchema: incompatible,
		}, nil
	case errors.Is(err, os.ErrNotExist):
		if err := replaceFile(path, state); err != nil {
			return nil, fmt.Errorf("initialize snapshot: %w", err)
		}
		return &Store{path: path, clock: clock, state: state}, nil
	default:
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
}

func (s *Store) Path() string {
	return s.path
}

func (s *Store) Revision() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state.Revision
}

func (s *Store) Read(ctx context.Context, fn func(*domain.State) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn(&s.state)
}

func (s *Store) Snapshot(ctx context.Context) (domain.State, error) {
	var snapshot domain.State
	err := s.Read(ctx, func(state *domain.State) error {
		copied, err := copyState(state)
		if err != nil {
			return err
		}
		snapshot = copied
		return nil
	})
	return snapshot, err
}

func (s *Store) Write(ctx context.Context, fn func(*domain.State) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	now := s.clock.Now().UTC()
	attempted := s.state.Revision + 1
	outcome := &CommitOutcome{At: now, AttemptedRev: attempted}
	if s.incompatibleSchema {
		err := domain.NewError(domain.CodeIncompatibleSchema, "snapshot schema is not supported by this build; refusing to rewrite it", map[string]any{
			"schema_version":    s.state.SchemaVersion,
			"supported_version": schemaVersion,
		})
		s.recordCommit(outcome, attempted, err)
		return err
	}
	next, err := copyState(&s.state)
	if err != nil {
		domainErr := domain.NewError(domain.CodeStorageFailure, "could not copy state", map[string]any{
			"reason": err.Error(),
		})
		s.recordCommit(outcome, attempted, domainErr)
		return domainErr
	}
	if err := fn(&next); err != nil {
		s.recordCommit(outcome, attempted, err)
		return err
	}
	next.SchemaVersion = schemaVersion
	next.Revision = attempted
	if err := replaceFile(s.path, next); err != nil {
		domainErr := domain.NewError(domain.CodeStorageFailure, "could not persist state", map[string]any{
			"reason": err.Error(),
		})
		s.recordCommit(outcome, attempted, domainErr)
		return domainErr
	}
	s.state = next
	s.recordCommit(outcome, attempted, nil)
	return nil
}

// recordCommit must be called with mu held.
func (s *Store) recordCommit(outcome *CommitOutcome, committed uint64, err error) {
	if err != nil {
		outcome.ErrorCode = string(domain.ErrorCodeOf(err))
		outcome.Error = truncateCommitError(err.Error())
	} else {
		outcome.Success = true
		outcome.CommittedRev = committed
	}
	s.lastCommit = outcome
}

// InspectHealth returns a coherent snapshot of revision, schema, entity
// counts, the last transaction outcome and storage accessibility. All
// in-memory fields and the durable storage checks are gathered while holding
// the read lock, so a concurrent Write cannot interleave and the report
// always describes one point in time. The checks create no plans, leases or
// audit events: writability is proven with a throwaway probe file that is
// removed before the inspection returns.
func (s *Store) InspectHealth(ctx context.Context) (HealthInspection, error) {
	var report HealthInspection
	err := s.Read(ctx, func(state *domain.State) error {
		now := s.clock.Now().UTC()
		report.CheckedAt = now
		report.Revision = state.Revision
		report.SchemaVersion = state.SchemaVersion
		report.SupportedSchema = schemaVersion
		report.IncompatibleSchema = s.incompatibleSchema
		report.Counts = HealthCounts{
			Plans:       boundedCount(len(state.Plans)),
			Revisions:   boundedCount(len(state.Revisions)),
			Nodes:       boundedCount(len(state.Nodes)),
			Leases:      boundedCount(len(state.Leases)),
			LeaseGroups: boundedCount(len(state.LeaseGroups)),
			Probes:      boundedCount(len(state.Probes)),
			Assessments: boundedCount(len(state.Assessments)),
			AuditEvents: boundedCount(len(state.AuditEvents)),
		}
		expired := 0
		for _, lease := range state.Leases {
			if lease.State == domain.LeaseClaimed && !lease.ExpiresAt.After(now) {
				expired++
			}
		}
		report.ExpiredActiveLeases = boundedCount(expired)
		report.Storage = s.probeStorageLocked(state)
		if s.lastCommit != nil {
			copied := *s.lastCommit
			report.LastCommit = &copied
		}
		return nil
	})
	return report, err
}

// probeStorageLocked performs the durable checks. The read lock is held, so no
// commit can replace the snapshot between the in-memory read and this probe.
func (s *Store) probeStorageLocked(state *domain.State) StorageInspection {
	storage := StorageInspection{Readable: true}
	payload, readErr := os.ReadFile(s.path)
	if readErr != nil {
		storage.Readable = false
		storage.ErrorCode = string(domain.CodeStorageFailure)
		storage.Error = truncateCommitError(readErr.Error())
		return storage
	}
	var durable domain.State
	if decodeErr := json.Unmarshal(payload, &durable); decodeErr != nil {
		storage.Readable = false
		storage.ErrorCode = string(domain.CodeStorageFailure)
		storage.Error = truncateCommitError(decodeErr.Error())
		return storage
	}
	storage.DurableRevision = durable.Revision
	storage.DurableSchemaVersion = durable.SchemaVersion
	storage.DurableFormatOK = durable.SchemaVersion == schemaVersion
	storage.DurableInSync = durable.Revision == state.Revision &&
		durable.SchemaVersion == state.SchemaVersion

	// Prove the snapshot location is writable without mutating business
	// state: write a uniquely named probe file, fsync it, and remove it. No
	// plan, lease, revision or audit event is ever created.
	if probeErr := probeDirectoryWrite(s.path); probeErr != nil {
		storage.Writable = false
		storage.ErrorCode = string(domain.CodeStorageFailure)
		storage.Error = truncateCommitError(probeErr.Error())
		return storage
	}
	storage.Writable = true
	return storage
}

func boundedCount(total int) BoundedCount {
	if total < 0 {
		total = 0
	}
	if total > HealthInspectionCapacity {
		return BoundedCount{
			Value:     HealthInspectionCapacity,
			Capacity:  HealthInspectionCapacity,
			Truncated: true,
		}
	}
	return BoundedCount{Value: total, Capacity: HealthInspectionCapacity}
}

func truncateCommitError(message string) string {
	const limit = 512
	runes := []rune(message)
	if len(runes) <= limit {
		return message
	}
	return string(runes[:limit])
}

func copyState(source *domain.State) (domain.State, error) {
	payload, err := json.Marshal(source)
	if err != nil {
		return domain.State{}, err
	}
	var copied domain.State
	if err := json.Unmarshal(payload, &copied); err != nil {
		return domain.State{}, err
	}
	return normalize(copied), nil
}

func AddAudit(
	state *domain.State,
	ids platform.IDGenerator,
	entityType string,
	entityID string,
	action string,
	at time.Time,
	attributes map[string]any,
) {
	state.AuditEvents = append(state.AuditEvents, domain.AuditEvent{
		ID:         ids.New("aud"),
		EntityType: entityType,
		EntityID:   entityID,
		Action:     action,
		At:         at.UTC(),
		Attributes: attributes,
	})
}
