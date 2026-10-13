package api

import "net/http"

type route struct {
	method  string
	pattern string
	role    string
	handler http.HandlerFunc
}

// routes is the whole API. Every route carries the role it requires; the
// handler is registered behind authorize, so a route cannot be added without
// deciding who may call it.
func (s *Server) routes() []route {
	routes := []route{
		{"GET", "/health", roleNone, s.health},
		{"GET", "/api/whoami", roleAny, s.whoami},

		{"POST", "/api/projects", roleAdmin, s.applyProject},
		{"GET", "/api/projects/{project}", roleRead, s.getProject},

		{"POST", "/api/executions", roleAdmin, s.submitExecution},
		{"GET", "/api/executions", roleRead, s.listExecutions},
		{"GET", "/api/executions/{id}", roleRead, s.getExecution},
		{"POST", "/api/executions/{id}/withdraw", roleAdmin, s.withdrawExecution},
		{"GET", "/api/attempts", roleRead, s.listAttempts},
		{"GET", "/api/attempts/{id}/log", roleRead, s.attemptLog},
		{"GET", "/api/events", roleRead, s.listEvents},

		{"GET", "/api/scopes", roleRead, s.listScopes},
		{"GET", "/api/scopes/{id}", roleRead, s.getScope},
		{"POST", "/api/scopes/{id}/pause", roleAdmin, s.pauseScope},
		{"POST", "/api/scopes/{id}/resume", roleAdmin, s.resumeScope},
		{"GET", "/api/pause-operations/{id}", roleRead, s.getPauseOperation},

		{"GET", "/api/nodes/quarantines", roleRead, s.listNodeQuarantines},
		{"POST", "/api/nodes/{id}/quarantine", roleAdmin, s.quarantineNode},
		{"DELETE", "/api/nodes/{id}/quarantine", roleAdmin, s.releaseNodeQuarantine},

		{"GET", "/api/resources", roleRead, s.resourceStatus},
		{"POST", "/api/resources/{id}/enable", roleAdmin, s.enableResource},
		{"POST", "/api/resources/{id}/quarantine", roleAdmin, s.quarantineResource},
		{"POST", "/api/resources/{id}/reconcile", roleAdmin, s.reconcileResource},

		{"POST", "/api/worker/heartbeat", roleWorker, s.workerHeartbeat},
		{"POST", "/api/worker/processes", roleWorker, s.workerProcess},
		{"GET", "/api/worker/commands", roleWorker, s.workerPoll},
		{"POST", "/api/worker/commands/{command}/acks", roleWorker, s.workerAck},

		{"POST", "/api/agent/logs", roleNode, s.agentLogAppend},
	}
	for _, op := range s.agentOps() {
		routes = append(routes, route{"POST", "/api/agent/" + op.name, roleNode, op.handler})
	}
	return routes
}
