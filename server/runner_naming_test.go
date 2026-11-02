package server

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDemandRunnerMetadataFromWorkflowJobSanitizesNames(t *testing.T) {
	metadata := demandRunnerMetadataFromWorkflowJob("fire-1x1", 123, "CI / Build", "Unit Tests (Node 22)")

	assert.Equal(t, "fire-1x1-ci-build-unit-tests-node-22-123", metadata.Prefix)
	assert.Equal(t, "CI / Build", metadata.WorkflowName)
	assert.Equal(t, "Unit Tests (Node 22)", metadata.JobName)
}

func TestPoolRunnerNameKeepsHostnameLengthLimit(t *testing.T) {
	pool := newTestPool(t, "fire-1x1", 1024, 1, NewCapacityManager(nil))

	name := pool.runnerName(strings.Repeat("long-workflow-name-", 10))

	assert.LessOrEqual(t, len(name), 63)
	assert.Contains(t, name, "-")
}
