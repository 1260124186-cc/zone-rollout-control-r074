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

type Store struct {
	mu             sync.RWMutex
	path           string
	state          domain.State
	schemaMismatch bool
	lastCommit     *CommitResult
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("snapshot path is required")
	}
	state := blankState()
	schemaMismatch := false
	payload, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(payload, &state); err != nil {
			return nil, fmt.Errorf("decode snapshot: %w", err)
		}
		schemaMismatch = state.SchemaVersion != SupportedSchemaVersion
		state = normalize(state)
	case errors.Is(err, os.ErrNotExist):
		if err := replaceFile(path, state); err != nil {
			return nil, fmt.Errorf("initialize snapshot: %w", err)
		}
	default:
		return nil, fmt.Errorf("read snapshot: %w", err)
	}
	// An unsupported schema version keeps the snapshot readable for health
	// diagnostics but blocks every commit so a newer format is never
	// rewritten (and possibly downgraded) by this process.
	return &Store{
		path:           path,
		state:          state,
		schemaMismatch: schemaMismatch,
	}, nil
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
	if s.schemaMismatch {
		return domain.NewError(domain.CodeSchemaIncompatible, "snapshot schema version is not supported by this process", map[string]any{
			"schema_version":           s.state.SchemaVersion,
			"supported_schema_version": SupportedSchemaVersion,
		})
	}
	next, err := copyState(&s.state)
	if err != nil {
		s.recordCommitLocked(false, s.state.Revision, "could not copy state: "+err.Error())
		return domain.NewError(domain.CodeStorageFailure, "could not copy state", map[string]any{
			"reason": err.Error(),
		})
	}
	if err := fn(&next); err != nil {
		return err
	}
	next.SchemaVersion = SupportedSchemaVersion
	next.Revision = s.state.Revision + 1
	if err := replaceFile(s.path, next); err != nil {
		s.recordCommitLocked(false, next.Revision, "could not persist state: "+err.Error())
		return domain.NewError(domain.CodeStorageFailure, "could not persist state", map[string]any{
			"reason": err.Error(),
		})
	}
	s.state = next
	s.recordCommitLocked(true, next.Revision, "")
	return nil
}

// recordCommitLocked records the outcome of the persistence phase of a
// transaction. Callers must hold s.mu.
func (s *Store) recordCommitLocked(success bool, revision uint64, detail string) {
	s.lastCommit = &CommitResult{
		Revision:    revision,
		Success:     success,
		Error:       detail,
		CommittedAt: time.Now().UTC(),
	}
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
