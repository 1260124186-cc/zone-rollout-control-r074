package httpapi

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/store"
)

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	inspection := s.store.InspectHealth(r.Context(), s.clock.Now())

	status := "ok"
	httpStatus := http.StatusOK
	switch {
	case !inspection.Storage.Readable:
		status = "unavailable"
		httpStatus = http.StatusServiceUnavailable
	case inspection.Storage.FormatIncompatible:
		status = "incompatible"
		httpStatus = http.StatusServiceUnavailable
	case !inspection.Storage.Writable:
		status = "read_only"
		httpStatus = http.StatusServiceUnavailable
	}

	// status stays the top-level field for compatibility; the embedded
	// inspection carries the full point-in-time health read.
	body := struct {
		Status string `json:"status"`
		store.HealthInspection
	}{
		Status:           status,
		HealthInspection: inspection,
	}
	sendData(w, r, httpStatus, body)
}
