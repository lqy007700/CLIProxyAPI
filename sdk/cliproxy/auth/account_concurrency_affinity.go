package auth

const (
	deferSessionAffinityBindingMetadataKey   = "cliproxy.internal.defer_session_affinity_binding"
	pendingSessionAffinityBindingMetadataKey = "cliproxy.internal.pending_session_affinity_binding"
)

type sessionAffinityBindingCommit func()

func applyOrDeferSessionAffinityBinding(metadata map[string]any, commit sessionAffinityBindingCommit) {
	if commit == nil {
		return
	}
	deferBinding, _ := metadata[deferSessionAffinityBindingMetadataKey].(bool)
	if !deferBinding {
		commit()
		return
	}
	metadata[pendingSessionAffinityBindingMetadataKey] = commit
}

func takePendingSessionAffinityBinding(metadata map[string]any) sessionAffinityBindingCommit {
	if len(metadata) == 0 {
		return nil
	}
	commit, _ := metadata[pendingSessionAffinityBindingMetadataKey].(sessionAffinityBindingCommit)
	delete(metadata, pendingSessionAffinityBindingMetadataKey)
	delete(metadata, deferSessionAffinityBindingMetadataKey)
	return commit
}
