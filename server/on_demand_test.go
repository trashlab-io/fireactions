package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	githubv63 "github.com/google/go-github/v63/github"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOnDemandRepositoryCacheRefreshesWithoutCachingFailures(t *testing.T) {
	server, _, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)
	now := time.Date(2026, 10, 7, 4, 0, 0, 0, time.UTC)
	controller.nowFn = func() time.Time { return now }
	calls := 0
	fail := false
	controller.listRepositoriesFn = func(context.Context, int64) ([]*githubv63.Repository, error) {
		calls++
		if fail {
			return nil, errors.New("temporary failure")
		}
		return []*githubv63.Repository{{ID: githubv63.Int64(int64(calls))}}, nil
	}

	first, err := controller.cachedRepositories(context.Background(), 1)
	require.NoError(t, err)
	now = now.Add(onDemandRepositoryCacheTTL - time.Second)
	cached, err := controller.cachedRepositories(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, first, cached)
	assert.Equal(t, 1, calls)

	_, err = controller.cachedRepositories(context.Background(), 2)
	require.NoError(t, err)
	assert.Equal(t, 2, calls, "installations must have separate caches")

	now = now.Add(time.Second)
	fail = true
	_, err = controller.cachedRepositories(context.Background(), 1)
	require.Error(t, err)
	fail = false
	refreshed, err := controller.cachedRepositories(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, 4, calls)
	assert.NotEqual(t, first, refreshed)
}

func TestOnDemandQuotaDelayPreservesDemandAndWebhooks(t *testing.T) {
	server, pool, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)
	controller.installationIDs["test-org"] = 1
	now := time.Date(2026, 10, 7, 4, 0, 0, 0, time.UTC)
	controller.nowFn = func() time.Time { return now }
	resetAt := now.Add(10 * time.Minute)
	controller.listRepositoriesFn = func(context.Context, int64) ([]*githubv63.Repository, error) {
		return []*githubv63.Repository{{
			Name:  githubv63.String("repo"),
			Owner: &githubv63.User{Login: githubv63.String("test-org")},
		}}, nil
	}
	controller.listWorkflowRunsFn = func(context.Context, int64, string, string, string) ([]*githubv63.WorkflowRun, error) {
		return nil, &githubv63.RateLimitError{
			Rate: githubv63.Rate{Reset: githubv63.Timestamp{Time: resetAt}},
			Response: &http.Response{StatusCode: http.StatusForbidden, Request: &http.Request{
				Method: http.MethodGet, URL: &url.URL{Scheme: "https", Host: "api.github.com"},
			}},
		}
	}
	event := &githubv63.WorkflowJobEvent{
		Action:      githubv63.String("queued"),
		Org:         &githubv63.Organization{Login: githubv63.String("test-org")},
		WorkflowJob: &githubv63.WorkflowJob{ID: githubv63.Int64(101), Labels: []string{"fire-1x1"}},
	}
	require.NoError(t, controller.processWorkflowJobEvent(event))
	assert.Equal(t, 1, pool.GetDesiredReplicas())

	delay := controller.reconcile(context.Background())
	assert.Equal(t, resetAt.Sub(now)+time.Second, delay)
	assert.Equal(t, 1, pool.GetDesiredReplicas(), "quota failures must not discard queued demand")

	event.Action = githubv63.String("completed")
	require.NoError(t, controller.processWorkflowJobEvent(event))
	assert.Equal(t, 0, pool.GetDesiredReplicas(), "webhooks must still update demand during the pause")

	now = resetAt.Add(time.Second)
	controller.listWorkflowRunsFn = func(context.Context, int64, string, string, string) ([]*githubv63.WorkflowRun, error) {
		return nil, nil
	}
	assert.Equal(t, controller.pollInterval, controller.reconcile(context.Background()))
}

func TestOnDemandWebhookScalesMatchingPool(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)

	payload := []byte(`{
		"action":"queued",
		"organization":{"login":"test-org"},
		"workflow_job":{"id":101,"labels":["self-hosted","fireactions","fire-1x1"]}
	}`)
	request := newSignedWebhookRequest(t, server.config.GitHub.WebhookSecret, payload)
	recorder := httptest.NewRecorder()

	controller.HandleGitHubWebhook(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code)
	assert.Equal(t, 1, poolA.GetDesiredReplicas())

	payload = []byte(`{
		"action":"completed",
		"organization":{"login":"test-org"},
		"workflow_job":{"id":101,"labels":["self-hosted","fireactions","fire-1x1"]}
	}`)
	request = newSignedWebhookRequest(t, server.config.GitHub.WebhookSecret, payload)
	recorder = httptest.NewRecorder()

	controller.HandleGitHubWebhook(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code)
	assert.Equal(t, 0, poolA.GetDesiredReplicas())
}

func TestOnDemandWebhookScalesPoolWhenJobOnlyRequestsPoolLabel(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)

	payload := []byte(`{
		"action":"queued",
		"organization":{"login":"test-org"},
		"workflow_job":{"id":102,"labels":["fire-1x1"]}
	}`)
	request := newSignedWebhookRequest(t, server.config.GitHub.WebhookSecret, payload)
	recorder := httptest.NewRecorder()

	controller.HandleGitHubWebhook(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code)
	assert.Equal(t, 1, poolA.GetDesiredReplicas())
}

func TestOnDemandWebhookStoresWorkflowRunnerNameMetadata(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)

	payload := []byte(`{
		"action":"queued",
		"organization":{"login":"test-org"},
		"workflow_job":{
			"id":103,
			"workflow_name":"CI",
			"name":"Unit Tests / Node 22",
			"labels":["self-hosted","fireactions","fire-1x1"]
		}
	}`)
	request := newSignedWebhookRequest(t, server.config.GitHub.WebhookSecret, payload)
	recorder := httptest.NewRecorder()

	controller.HandleGitHubWebhook(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code)
	assert.Equal(t, 1, poolA.GetDesiredReplicas())

	metadata := poolA.nextDemandRunnerMetadata()
	assert.Equal(t, "CI", metadata.WorkflowName)
	assert.Equal(t, "Unit Tests / Node 22", metadata.JobName)
	assert.Equal(t, "fire-1x1-ci-unit-tests-node-22-103", metadata.Prefix)
}

func TestOnDemandWebhookRejectsInvalidSignature(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)

	payload := []byte(`{
		"action":"queued",
		"organization":{"login":"test-org"},
		"workflow_job":{"id":101,"labels":["self-hosted","fireactions","fire-1x1"]}
	}`)
	request := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-GitHub-Event", "workflow_job")
	request.Header.Set(githubv63.SHA256SignatureHeader, "sha256=invalid")

	recorder := httptest.NewRecorder()
	controller.HandleGitHubWebhook(recorder, request)

	assert.Equal(t, http.StatusUnauthorized, recorder.Code)
	assert.Equal(t, 0, poolA.GetDesiredReplicas())
}

func TestOnDemandReconcileRebuildsPoolDemand(t *testing.T) {
	server, _, poolB := newOnDemandTestServer(t)
	controller := newOnDemandController(server)
	controller.installationIDs["test-org"] = 1
	controller.listRepositoriesFn = func(ctx context.Context, installationID int64) ([]*githubv63.Repository, error) {
		return []*githubv63.Repository{{
			Name: githubv63.String("repo"),
			Owner: &githubv63.User{
				Login: githubv63.String("test-org"),
			},
		}}, nil
	}
	controller.listWorkflowRunsFn = func(ctx context.Context, installationID int64, owner, repo, status string) ([]*githubv63.WorkflowRun, error) {
		if status != "queued" {
			return nil, nil
		}

		return []*githubv63.WorkflowRun{{
			ID: githubv63.Int64(55),
		}}, nil
	}
	controller.listWorkflowJobsFn = func(ctx context.Context, installationID int64, owner, repo string, runID int64) ([]*githubv63.WorkflowJob, error) {
		return []*githubv63.WorkflowJob{{
			ID:     githubv63.Int64(201),
			Status: githubv63.String("queued"),
			Labels: []string{"fire-4x16"},
		}}, nil
	}

	require.NoError(t, controller.reconcileOnce(context.Background()))
	assert.Equal(t, 1, poolB.GetDesiredReplicas())
}

func TestOnDemandReconcilePreservesWebhookTrackedJobWithinMissingGrace(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)
	controller.installationIDs["test-org"] = 1

	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	controller.nowFn = func() time.Time { return now }
	controller.listRepositoriesFn = func(ctx context.Context, installationID int64) ([]*githubv63.Repository, error) {
		return []*githubv63.Repository{{
			Name: githubv63.String("repo"),
			Owner: &githubv63.User{
				Login: githubv63.String("test-org"),
			},
		}}, nil
	}
	controller.listWorkflowRunsFn = func(ctx context.Context, installationID int64, owner, repo, status string) ([]*githubv63.WorkflowRun, error) {
		return nil, nil
	}
	controller.listWorkflowJobsFn = func(ctx context.Context, installationID int64, owner, repo string, runID int64) ([]*githubv63.WorkflowJob, error) {
		return nil, nil
	}

	err := controller.processWorkflowJobEvent(&githubv63.WorkflowJobEvent{
		Action: githubv63.String("in_progress"),
		Org:    &githubv63.Organization{Login: githubv63.String("test-org")},
		WorkflowJob: &githubv63.WorkflowJob{
			ID:     githubv63.Int64(301),
			Labels: []string{"self-hosted", "fireactions", "fire-1x1"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, poolA.GetDesiredReplicas())

	now = now.Add(onDemandMissingJobGrace - time.Second)
	require.NoError(t, controller.reconcileOnce(context.Background()))
	assert.Equal(t, 1, poolA.GetDesiredReplicas())

	now = now.Add(2 * time.Second)
	require.NoError(t, controller.reconcileOnce(context.Background()))
	assert.Equal(t, 0, poolA.GetDesiredReplicas())
}

func TestOnDemandCompletedWebhookBeatsStaleReconcileResult(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)
	controller.installationIDs["test-org"] = 1

	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	controller.nowFn = func() time.Time { return now }
	controller.listRepositoriesFn = func(ctx context.Context, installationID int64) ([]*githubv63.Repository, error) {
		return []*githubv63.Repository{{
			Name: githubv63.String("repo"),
			Owner: &githubv63.User{
				Login: githubv63.String("test-org"),
			},
		}}, nil
	}
	controller.listWorkflowRunsFn = func(ctx context.Context, installationID int64, owner, repo, status string) ([]*githubv63.WorkflowRun, error) {
		if status != "in_progress" {
			return nil, nil
		}

		return []*githubv63.WorkflowRun{{
			ID: githubv63.Int64(88),
		}}, nil
	}
	controller.listWorkflowJobsFn = func(ctx context.Context, installationID int64, owner, repo string, runID int64) ([]*githubv63.WorkflowJob, error) {
		return []*githubv63.WorkflowJob{{
			ID:     githubv63.Int64(302),
			Status: githubv63.String("in_progress"),
			Labels: []string{"self-hosted", "fireactions", "fire-1x1"},
		}}, nil
	}

	err := controller.processWorkflowJobEvent(&githubv63.WorkflowJobEvent{
		Action: githubv63.String("in_progress"),
		Org:    &githubv63.Organization{Login: githubv63.String("test-org")},
		WorkflowJob: &githubv63.WorkflowJob{
			ID:     githubv63.Int64(302),
			Labels: []string{"self-hosted", "fireactions", "fire-1x1"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, poolA.GetDesiredReplicas())

	now = now.Add(time.Second)
	err = controller.processWorkflowJobEvent(&githubv63.WorkflowJobEvent{
		Action: githubv63.String("completed"),
		Org:    &githubv63.Organization{Login: githubv63.String("test-org")},
		WorkflowJob: &githubv63.WorkflowJob{
			ID:     githubv63.Int64(302),
			Labels: []string{"self-hosted", "fireactions", "fire-1x1"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, poolA.GetDesiredReplicas())

	now = now.Add(time.Second)
	require.NoError(t, controller.reconcileOnce(context.Background()))
	assert.Equal(t, 0, poolA.GetDesiredReplicas())
}

func TestOnDemandReconcileDropsJobAssignedToAnotherHost(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)
	controller.installationIDs["test-org"] = 1

	now := time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)
	controller.nowFn = func() time.Time { return now }
	controller.listRepositoriesFn = func(ctx context.Context, installationID int64) ([]*githubv63.Repository, error) {
		return []*githubv63.Repository{{
			Name: githubv63.String("repo"),
			Owner: &githubv63.User{
				Login: githubv63.String("test-org"),
			},
		}}, nil
	}
	controller.listWorkflowRunsFn = func(ctx context.Context, installationID int64, owner, repo, status string) ([]*githubv63.WorkflowRun, error) {
		if status != "in_progress" {
			return nil, nil
		}

		return []*githubv63.WorkflowRun{{
			ID: githubv63.Int64(88),
		}}, nil
	}
	controller.listWorkflowJobsFn = func(ctx context.Context, installationID int64, owner, repo string, runID int64) ([]*githubv63.WorkflowJob, error) {
		return []*githubv63.WorkflowJob{{
			ID:         githubv63.Int64(401),
			Status:     githubv63.String("in_progress"),
			Labels:     []string{"self-hosted", "fireactions", "fire-1x1"},
			RunnerID:   githubv63.Int64(999),
			RunnerName: githubv63.String("other-host-runner"),
		}}, nil
	}

	err := controller.processWorkflowJobEvent(&githubv63.WorkflowJobEvent{
		Action: githubv63.String("queued"),
		Org:    &githubv63.Organization{Login: githubv63.String("test-org")},
		WorkflowJob: &githubv63.WorkflowJob{
			ID:     githubv63.Int64(401),
			Labels: []string{"self-hosted", "fireactions", "fire-1x1"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, poolA.GetDesiredReplicas())

	now = now.Add(time.Second)
	require.NoError(t, controller.reconcileOnce(context.Background()))
	assert.Equal(t, 0, poolA.GetDesiredReplicas())
}

func TestOnDemandReconcileKeepsJobAssignedToLocalRunner(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)
	controller.installationIDs["test-org"] = 1

	poolA.machinesMu.Lock()
	poolA.machines["local-runner"] = &Machine{
		Name:     "local-runner",
		RunnerID: 777,
		Pool:     poolA.config.Name,
	}
	poolA.machinesMu.Unlock()

	controller.listRepositoriesFn = func(ctx context.Context, installationID int64) ([]*githubv63.Repository, error) {
		return []*githubv63.Repository{{
			Name: githubv63.String("repo"),
			Owner: &githubv63.User{
				Login: githubv63.String("test-org"),
			},
		}}, nil
	}
	controller.listWorkflowRunsFn = func(ctx context.Context, installationID int64, owner, repo, status string) ([]*githubv63.WorkflowRun, error) {
		if status != "in_progress" {
			return nil, nil
		}

		return []*githubv63.WorkflowRun{{
			ID: githubv63.Int64(89),
		}}, nil
	}
	controller.listWorkflowJobsFn = func(ctx context.Context, installationID int64, owner, repo string, runID int64) ([]*githubv63.WorkflowJob, error) {
		return []*githubv63.WorkflowJob{{
			ID:         githubv63.Int64(402),
			Status:     githubv63.String("in_progress"),
			Labels:     []string{"self-hosted", "fireactions", "fire-1x1"},
			RunnerID:   githubv63.Int64(777),
			RunnerName: githubv63.String("local-runner"),
		}}, nil
	}

	require.NoError(t, controller.reconcileOnce(context.Background()))
	assert.Equal(t, 1, poolA.GetDesiredReplicas())
}

func TestOnDemandWebhookDropsJobAssignedToAnotherHost(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	controller := newOnDemandController(server)

	err := controller.processWorkflowJobEvent(&githubv63.WorkflowJobEvent{
		Action: githubv63.String("queued"),
		Org:    &githubv63.Organization{Login: githubv63.String("test-org")},
		WorkflowJob: &githubv63.WorkflowJob{
			ID:     githubv63.Int64(403),
			Labels: []string{"self-hosted", "fireactions", "fire-1x1"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, poolA.GetDesiredReplicas())

	err = controller.processWorkflowJobEvent(&githubv63.WorkflowJobEvent{
		Action: githubv63.String("in_progress"),
		Org:    &githubv63.Organization{Login: githubv63.String("test-org")},
		WorkflowJob: &githubv63.WorkflowJob{
			ID:         githubv63.Int64(403),
			Labels:     []string{"self-hosted", "fireactions", "fire-1x1"},
			RunnerID:   githubv63.Int64(999),
			RunnerName: githubv63.String("other-host-runner"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, poolA.GetDesiredReplicas())
}

func newOnDemandTestServer(t *testing.T) (*Server, *Pool, *Pool) {
	t.Helper()

	poolA := newTestPool(t, "fire-1x1", 1024, 1, NewCapacityManager(nil))
	poolB := newTestPool(t, "fire-4x16", 16384, 4, NewCapacityManager(nil))

	logger := zerolog.New(bytes.NewBuffer(nil))
	server := &Server{
		config: &Config{
			OnDemand: true,
			GitHub: &GitHubConfig{
				WebhookSecret: "secret",
			},
			Metrics: &MetricsConfig{
				Address: "127.0.0.1:8081",
			},
		},
		pools: map[string]*Pool{
			poolA.config.Name: poolA,
			poolB.config.Name: poolB,
		},
		l:      &sync.Mutex{},
		logger: &logger,
	}

	return server, poolA, poolB
}

func newSignedWebhookRequest(t *testing.T, secret string, payload []byte) *http.Request {
	t.Helper()

	mac := hmac.New(sha256.New, []byte(secret))
	_, err := mac.Write(payload)
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodPost, "/webhooks/github", bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-GitHub-Event", "workflow_job")
	request.Header.Set(githubv63.SHA256SignatureHeader, "sha256="+hex.EncodeToString(mac.Sum(nil)))
	return request
}

func TestJobInDemandShardAssignsEachJobToExactlyOneHost(t *testing.T) {
	createdAt := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	for _, elapsed := range []time.Duration{0, 4 * time.Minute, 5 * time.Minute, 17 * time.Minute, time.Hour} {
		now := createdAt.Add(elapsed)
		for jobID := int64(1); jobID <= 50; jobID++ {
			owners := 0
			for index := 0; index < 3; index++ {
				shard := &OnDemandShardConfig{Count: 3, Index: index, RotateAfter: 5 * time.Minute}
				if jobInDemandShard(shard, jobID, createdAt, now) {
					owners++
				}
			}
			assert.Equal(t, 1, owners, "job %d after %s", jobID, elapsed)
		}
	}
}

func TestJobInDemandShardRotatesOwner(t *testing.T) {
	createdAt := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	owner := func(now time.Time) int {
		for index := 0; index < 3; index++ {
			shard := &OnDemandShardConfig{Count: 3, Index: index, RotateAfter: 5 * time.Minute}
			if jobInDemandShard(shard, 7, createdAt, now) {
				return index
			}
		}
		return -1
	}

	first := owner(createdAt.Add(time.Minute))
	assert.Equal(t, 1, first)
	assert.Equal(t, first, owner(createdAt.Add(4*time.Minute)))
	assert.Equal(t, 2, owner(createdAt.Add(5*time.Minute)))
	assert.Equal(t, 0, owner(createdAt.Add(10*time.Minute)))
	assert.Equal(t, first, owner(createdAt.Add(15*time.Minute)))
}

func TestJobInDemandShardDisabledCountsEveryJob(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	assert.True(t, jobInDemandShard(nil, 8, now, now))
	assert.True(t, jobInDemandShard(&OnDemandShardConfig{Count: 1}, 8, now, now))
}

func TestOnDemandReconcileCountsOnlyQueuedJobsInLocalShard(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	server.config.OnDemandShard = &OnDemandShardConfig{Count: 3, Index: 0, RotateAfter: 5 * time.Minute}
	controller := newOnDemandController(server)
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	controller.nowFn = func() time.Time { return now }
	controller.installationIDs["test-org"] = 1
	createdAt := githubv63.Timestamp{Time: now.Add(-time.Minute)}

	controller.listRepositoriesFn = func(ctx context.Context, installationID int64) ([]*githubv63.Repository, error) {
		return []*githubv63.Repository{{
			Name: githubv63.String("repo"),
			Owner: &githubv63.User{
				Login: githubv63.String("test-org"),
			},
		}}, nil
	}
	controller.listWorkflowRunsFn = func(ctx context.Context, installationID int64, owner, repo, status string) ([]*githubv63.WorkflowRun, error) {
		if status != "queued" {
			return nil, nil
		}

		return []*githubv63.WorkflowRun{{
			ID: githubv63.Int64(91),
		}}, nil
	}
	controller.listWorkflowJobsFn = func(ctx context.Context, installationID int64, owner, repo string, runID int64) ([]*githubv63.WorkflowJob, error) {
		jobs := make([]*githubv63.WorkflowJob, 0, 6)
		for jobID := int64(300); jobID < 306; jobID++ {
			jobs = append(jobs, &githubv63.WorkflowJob{
				ID:        githubv63.Int64(jobID),
				Status:    githubv63.String("queued"),
				Labels:    []string{"fire-1x1"},
				CreatedAt: &createdAt,
			})
		}
		return jobs, nil
	}

	require.NoError(t, controller.reconcileOnce(context.Background()))
	// Jobs 300 and 303 are in shard 0 for the first rotation interval.
	assert.Equal(t, 2, poolA.GetDesiredReplicas())

	now = now.Add(5 * time.Minute)
	require.NoError(t, controller.reconcileOnce(context.Background()))
	// After one rotation, jobs 302 and 305 move to shard 0.
	assert.Equal(t, 2, poolA.GetDesiredReplicas())
	shard := server.config.OnDemandShard
	assert.True(t, jobInDemandShard(shard, 302, createdAt.Time, now))
	assert.True(t, jobInDemandShard(shard, 305, createdAt.Time, now))
	assert.False(t, jobInDemandShard(shard, 300, createdAt.Time, now))
}

func TestOnDemandShardAlwaysCountsJobOnLocalRunner(t *testing.T) {
	server, poolA, _ := newOnDemandTestServer(t)
	server.config.OnDemandShard = &OnDemandShardConfig{Count: 3, Index: 0, RotateAfter: 5 * time.Minute}
	controller := newOnDemandController(server)
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	controller.nowFn = func() time.Time { return now }

	poolA.machinesMu.Lock()
	poolA.machines["local-runner"] = &Machine{
		Name:     "local-runner",
		RunnerID: 778,
		Pool:     poolA.config.Name,
	}
	poolA.machinesMu.Unlock()

	createdAt := githubv63.Timestamp{Time: now}
	// Job 301 is in shard 1, but it runs on a runner of this host.
	require.NoError(t, controller.processWorkflowJobEvent(&githubv63.WorkflowJobEvent{
		Action: githubv63.String("in_progress"),
		Org:    &githubv63.Organization{Login: githubv63.String("test-org")},
		WorkflowJob: &githubv63.WorkflowJob{
			ID:         githubv63.Int64(301),
			Labels:     []string{"fire-1x1"},
			RunnerID:   githubv63.Int64(778),
			RunnerName: githubv63.String("local-runner"),
			CreatedAt:  &createdAt,
		},
	}))
	assert.Equal(t, 1, poolA.GetDesiredReplicas())
}
