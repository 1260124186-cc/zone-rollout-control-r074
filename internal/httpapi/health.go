package httpapi

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/health"
)

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	report, err := s.healthCheck.Inspect(r.Context())
	if err != nil {
		sendError(w, r, err)
		return
	}
	status := http.StatusOK
	if report.Status != health.StatusOK {
		status = http.StatusServiceUnavailable
	}
	sendData(w, r, status, report)
}
