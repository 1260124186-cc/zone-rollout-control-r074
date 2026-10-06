package httpapi

import (
	"log/slog"
	"net/http"

	"solo-0014-zone-rollout-control/internal/allocation"
	"solo-0014-zone-rollout-control/internal/assessment"
	"solo-0014-zone-rollout-control/internal/fleet"
	"solo-0014-zone-rollout-control/internal/plan"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/probe"
	"solo-0014-zone-rollout-control/internal/store"
)

type Server struct {
	store       *store.Store
	plans       *plan.Service
	fleet       *fleet.Service
	allocations *allocation.Service
	probes      *probe.Service
	assessments *assessment.Service
	ids         platform.IDGenerator
	clock       platform.Clock
	logger      *slog.Logger
}

func New(
	stateStore *store.Store,
	plans *plan.Service,
	fleetService *fleet.Service,
	allocations *allocation.Service,
	probes *probe.Service,
	assessments *assessment.Service,
	ids platform.IDGenerator,
	clock platform.Clock,
	logger *slog.Logger,
) *Server {
	return &Server{
		store:       stateStore,
		plans:       plans,
		fleet:       fleetService,
		allocations: allocations,
		probes:      probes,
		assessments: assessments,
		ids:         ids,
		clock:       clock,
		logger:      logger,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.routes(mux)
	return s.recoverer(s.withRequestID(s.withLogging(mux)))
}
