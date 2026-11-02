package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	githubv63 "github.com/google/go-github/v63/github"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type onDemandRunFixture struct {
	Run  *githubv63.WorkflowRun   `json:"run"`
	Jobs []*githubv63.WorkflowJob `json:"jobs"`
}

// fakeWorkflowRunsAPI answers run and job list calls like the GitHub API: a run
// is only returned for the status filter that matches its current status.
type fakeWorkflowRunsAPI struct {
	runs        []*githubv63.WorkflowRun
	jobs        map[int64][]*githubv63.WorkflowJob
	runListCall map[string]int
	jobListCall map[int64]int
}

func newFakeWorkflowRunsAPI(fixtures ...onDemandRunFixture) *fakeWorkflowRunsAPI {
	api := &fakeWorkflowRunsAPI{
		jobs:        make(map[int64][]*githubv63.WorkflowJob),
		runListCall: make(map[string]int),
		jobListCall: make(map[int64]int),
	}
	for _, fixture := range fixtures {
		api.runs = append(api.runs, fixture.Run)
		api.jobs[fixture.Run.GetID()] = fixture.Jobs
	}
	return api
}

func (a *fakeWorkflowRunsAPI) install(controller *onDemandController) {
	controller.installationIDs["test-org"] = 1
	controller.listRepositoriesFn = func(context.Context, int64) ([]*githubv63.Repository, error) {
		return []*githubv63.Repository{{
			Name:  githubv63.String("repo"),
			Owner: &githubv63.User{Login: githubv63.String("test-org")},
		}}, nil
	}
	controller.listWorkflowRunsFn = func(_ context.Context, _ int64, _, _, status string) ([]*githubv63.WorkflowRun, error) {
		a.runListCall[status]++
		var runs []*githubv63.WorkflowRun
		for _, run := range a.runs {
			if run.GetStatus() == status {
				runs = append(runs, run)
			}
		}
		return runs, nil
	}
	controller.listWorkflowJobsFn = func(_ context.Context, _ int64, _, _ string, runID int64) ([]*githubv63.WorkflowJob, error) {
		a.jobListCall[runID]++
		return a.jobs[runID], nil
	}
}

func loadOnDemandRunFixture(t *testing.T, name string) onDemandRunFixture {
	t.Helper()

	data, err := os.ReadFile(filepath.Join("testdata", "on_demand", name))
	require.NoError(t, err)

	var fixture onDemandRunFixture
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.NotNil(t, fixture.Run)
	return fixture
}

func newOnDemandScanTestController(t *testing.T) (*onDemandController, *Pool) {
	t.Helper()

	server, _, _ := newOnDemandTestServer(t)
	pool := newTestPool(t, "fire-1x2", 2048, 1, NewCapacityManager(nil))
	server.pools[pool.config.Name] = pool
	return newOnDemandController(server), pool
}

func withOnDemandWorkflowRunStatuses(t *testing.T, statuses []string) {
	t.Helper()

	previous := onDemandWorkflowRunStatuses
	onDemandWorkflowRunStatuses = statuses
	t.Cleanup(func() { onDemandWorkflowRunStatuses = previous })
}

func TestOnDemandReconcileFindsQueuedJobInPendingRunReplay(t *testing.T) {
	for _, name := range []string{
		"run_41395664158_pending_queued_job.json",
		"run_41115104748_pending_queued_job.json",
	} {
		t.Run(name, func(t *testing.T) {
			fixture := loadOnDemandRunFixture(t, name)
			require.Equal(t, "pending", fixture.Run.GetStatus())
			require.Len(t, fixture.Jobs, 1)
			require.Equal(t, "queued", fixture.Jobs[0].GetStatus())

			t.Run("previous statuses miss the job", func(t *testing.T) {
				withOnDemandWorkflowRunStatuses(t, []string{"queued", "in_progress"})
				controller, pool := newOnDemandScanTestController(t)
				api := newFakeWorkflowRunsAPI(fixture)
				api.install(controller)

				require.NoError(t, controller.reconcileOnce(context.Background()))
				assert.Equal(t, 0, pool.GetDesiredReplicas())
				assert.Zero(t, api.jobListCall[fixture.Run.GetID()])
			})

			t.Run("current statuses find the job", func(t *testing.T) {
				controller, pool := newOnDemandScanTestController(t)
				api := newFakeWorkflowRunsAPI(fixture)
				api.install(controller)

				require.NoError(t, controller.reconcileOnce(context.Background()))
				assert.Equal(t, 1, pool.GetDesiredReplicas())
				assert.Equal(t, 1, api.jobListCall[fixture.Run.GetID()])
			})
		})
	}
}

func TestOnDemandReconcileScansEveryRunStatusOnce(t *testing.T) {
	controller, _ := newOnDemandScanTestController(t)
	api := newFakeWorkflowRunsAPI()
	api.install(controller)

	require.NoError(t, controller.reconcileOnce(context.Background()))
	assert.Equal(t, map[string]int{"queued": 1, "in_progress": 1, "pending": 1, "waiting": 1}, api.runListCall)
}

func TestOnDemandReconcileIgnoresJobsBlockedInPendingOrWaitingRuns(t *testing.T) {
	controller, pool := newOnDemandScanTestController(t)
	api := newFakeWorkflowRunsAPI(
		onDemandRunFixture{
			// A run held by a concurrency group: its jobs are pending, not queued.
			Run: &githubv63.WorkflowRun{ID: githubv63.Int64(1), Status: githubv63.String("pending")},
			Jobs: []*githubv63.WorkflowJob{{
				ID: githubv63.Int64(11), Status: githubv63.String("pending"), Labels: []string{"fire-1x2"},
			}},
		},
		onDemandRunFixture{
			// A run waiting for environment approval: the gated job waits and a
			// sibling job that does not need approval is already queued.
			Run: &githubv63.WorkflowRun{ID: githubv63.Int64(2), Status: githubv63.String("waiting")},
			Jobs: []*githubv63.WorkflowJob{
				{ID: githubv63.Int64(21), Status: githubv63.String("waiting"), Labels: []string{"fire-1x2"}},
				{ID: githubv63.Int64(22), Status: githubv63.String("queued"), Labels: []string{"fire-1x2"}},
				{ID: githubv63.Int64(23), Status: githubv63.String("completed"), Labels: []string{"fire-1x2"}},
				{ID: githubv63.Int64(24), Status: githubv63.String("queued"), Labels: []string{"ubuntu-latest"}},
			},
		},
	)
	api.install(controller)

	require.NoError(t, controller.reconcileOnce(context.Background()))
	assert.Equal(t, 1, pool.GetDesiredReplicas(), "only the queued job with a pool label creates demand")
}

func TestOnDemandReconcileDropsJobInWaitingRunAssignedToAnotherHost(t *testing.T) {
	controller, pool := newOnDemandScanTestController(t)
	api := newFakeWorkflowRunsAPI(onDemandRunFixture{
		Run: &githubv63.WorkflowRun{ID: githubv63.Int64(3), Status: githubv63.String("waiting")},
		Jobs: []*githubv63.WorkflowJob{{
			ID:         githubv63.Int64(31),
			Status:     githubv63.String("in_progress"),
			Labels:     []string{"fire-1x2"},
			RunnerID:   githubv63.Int64(777),
			RunnerName: githubv63.String("fire-1x2-other-host"),
		}},
	})
	api.install(controller)

	require.NoError(t, controller.reconcileOnce(context.Background()))
	assert.Equal(t, 0, pool.GetDesiredReplicas())
}

func TestOnDemandReconcileListsJobsOnceWhenRunAppearsUnderTwoStatuses(t *testing.T) {
	controller, pool := newOnDemandScanTestController(t)
	run := &githubv63.WorkflowRun{ID: githubv63.Int64(4), Status: githubv63.String("pending")}
	api := newFakeWorkflowRunsAPI(onDemandRunFixture{
		Run: run,
		Jobs: []*githubv63.WorkflowJob{{
			ID: githubv63.Int64(41), Status: githubv63.String("queued"), Labels: []string{"fire-1x2"},
		}},
	})
	api.install(controller)
	listRuns := controller.listWorkflowRunsFn
	controller.listWorkflowRunsFn = func(ctx context.Context, installationID int64, owner, repo, status string) ([]*githubv63.WorkflowRun, error) {
		// The run moves from queued to pending between the two list calls.
		if status == "queued" {
			api.runListCall[status]++
			return []*githubv63.WorkflowRun{run}, nil
		}
		return listRuns(ctx, installationID, owner, repo, status)
	}

	require.NoError(t, controller.reconcileOnce(context.Background()))
	assert.Equal(t, 1, api.jobListCall[run.GetID()])
	assert.Equal(t, 1, pool.GetDesiredReplicas())
}
