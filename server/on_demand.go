package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	githubv63 "github.com/google/go-github/v63/github"
	"github.com/rs/zerolog"
)

const (
	onDemandReconcileInterval  = 2 * time.Minute
	onDemandRepositoryCacheTTL = 15 * time.Minute
	onDemandMissingJobGrace    = 2 * onDemandReconcileInterval
	onDemandCompletedJobWindow = 2 * onDemandReconcileInterval
)

type repositoryCacheEntry struct {
	repositories []*githubv63.Repository
	expiresAt    time.Time
}

type jobDemandEntry struct {
	organization string
	poolName     string
	runner       demandRunnerMetadata
	lastSeenAt   time.Time
}

type poolDemandEntry struct {
	count   int
	runners []demandRunnerMetadata
}

type onDemandController struct {
	server        *Server
	logger        *zerolog.Logger
	webhookSecret []byte
	pollInterval  time.Duration

	mu              sync.Mutex
	jobs            map[int64]*jobDemandEntry
	completedJobs   map[int64]time.Time
	installationIDs map[string]int64
	repositoryCache map[int64]repositoryCacheEntry
	nowFn           func() time.Time

	listRepositoriesFn func(context.Context, int64) ([]*githubv63.Repository, error)
	listWorkflowRunsFn func(context.Context, int64, string, string, string) ([]*githubv63.WorkflowRun, error)
	listWorkflowJobsFn func(context.Context, int64, string, string, int64) ([]*githubv63.WorkflowJob, error)
}

func newOnDemandController(server *Server) *onDemandController {
	logger := server.logger.With().Str("component", "on-demand").Logger()
	controller := &onDemandController{
		server:          server,
		logger:          &logger,
		webhookSecret:   []byte(server.config.GitHub.WebhookSecret),
		pollInterval:    onDemandReconcileInterval,
		jobs:            make(map[int64]*jobDemandEntry),
		completedJobs:   make(map[int64]time.Time),
		installationIDs: make(map[string]int64),
		repositoryCache: make(map[int64]repositoryCacheEntry),
		nowFn:           time.Now,
	}
	controller.listRepositoriesFn = controller.listRepositories
	controller.listWorkflowRunsFn = controller.listWorkflowRuns
	controller.listWorkflowJobsFn = controller.listWorkflowJobs
	return controller
}

func (c *onDemandController) Initialize(ctx context.Context) error {
	pools := c.server.snapshotPools()
	for _, pool := range pools {
		metricOnDemandJobsActive.WithLabelValues(pool.config.Name, pool.config.Runner.Organization).Set(0)

		if _, ok := c.installationIDs[pool.config.Runner.Organization]; ok {
			continue
		}

		installation, _, err := c.server.github.Apps.FindOrganizationInstallation(ctx, pool.config.Runner.Organization)
		if err != nil {
			return fmt.Errorf("finding GitHub installation for organization %q: %w", pool.config.Runner.Organization, err)
		}

		c.installationIDs[pool.config.Runner.Organization] = installation.GetID()
	}

	return nil
}

func (c *onDemandController) Run(ctx context.Context) {
	go func() {
		for {
			delay := c.reconcile(ctx)
			// Wait after the scan finishes so slow scans cannot trigger back-to-back polling.
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
}

func (c *onDemandController) HandleGitHubWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	payload, err := githubv63.ValidatePayload(r, c.webhookSecret)
	if err != nil {
		metricOnDemandWebhookEvents.WithLabelValues("unknown", "invalid_signature").Inc()
		c.logger.Warn().Err(err).Msg("Rejected GitHub webhook with invalid signature")
		http.Error(w, "invalid webhook signature", http.StatusUnauthorized)
		return
	}

	webhookType := githubv63.WebHookType(r)
	if webhookType != "workflow_job" {
		metricOnDemandWebhookEvents.WithLabelValues(webhookType, "ignored").Inc()
		w.WriteHeader(http.StatusAccepted)
		return
	}

	event, err := githubv63.ParseWebHook(webhookType, payload)
	if err != nil {
		metricOnDemandWebhookEvents.WithLabelValues(webhookType, "parse_error").Inc()
		c.logger.Error().Err(err).Msg("Failed to parse GitHub workflow_job webhook")
		http.Error(w, "invalid webhook payload", http.StatusBadRequest)
		return
	}

	workflowJobEvent, ok := event.(*githubv63.WorkflowJobEvent)
	if !ok {
		metricOnDemandWebhookEvents.WithLabelValues(webhookType, "parse_error").Inc()
		http.Error(w, "invalid workflow_job payload", http.StatusBadRequest)
		return
	}

	if err := c.processWorkflowJobEvent(workflowJobEvent); err != nil {
		metricOnDemandWebhookEvents.WithLabelValues(workflowJobEvent.GetAction(), "error").Inc()
		c.logger.Error().Err(err).Msg("Failed to process GitHub workflow_job webhook")
		http.Error(w, "failed to process webhook", http.StatusInternalServerError)
		return
	}

	metricOnDemandWebhookEvents.WithLabelValues(workflowJobEvent.GetAction(), "accepted").Inc()
	w.WriteHeader(http.StatusAccepted)
}

func (c *onDemandController) processWorkflowJobEvent(event *githubv63.WorkflowJobEvent) error {
	if event == nil || event.WorkflowJob == nil {
		return nil
	}

	jobID := event.WorkflowJob.GetID()
	if jobID == 0 {
		return nil
	}

	switch event.GetAction() {
	case "queued", "in_progress":
		organization := c.organizationFromEvent(event)
		poolName, matchResult := c.matchPool(organization, event.WorkflowJob.Labels)
		if matchResult != "matched" {
			c.logger.Info().
				Int64("job_id", jobID).
				Str("organization", organization).
				Str("result", matchResult).
				Strs("labels", event.WorkflowJob.Labels).
				Msg("Ignoring workflow job for on-demand scaling")
			return nil
		}

		if event.GetAction() == "in_progress" && c.workflowJobAssignedToAnotherHost(event.WorkflowJob) {
			c.logger.Info().
				Int64("job_id", jobID).
				Int64("runner_id", event.WorkflowJob.GetRunnerID()).
				Str("runner_name", event.WorkflowJob.GetRunnerName()).
				Str("pool", poolName).
				Str("organization", organization).
				Msg("Dropping workflow job demand assigned to another host")
			c.dropJob(jobID)
			return nil
		}

		c.logger.Info().
			Int64("job_id", jobID).
			Str("pool", poolName).
			Str("organization", organization).
			Str("action", event.GetAction()).
			Msg("Matched workflow job to pool")
		c.upsertJob(jobID, &jobDemandEntry{
			organization: organization,
			poolName:     poolName,
			runner: demandRunnerMetadataFromWorkflowJob(
				poolName,
				jobID,
				event.WorkflowJob.GetWorkflowName(),
				event.WorkflowJob.GetName(),
			),
		})
	case "completed":
		c.removeJob(jobID)
	default:
		return nil
	}

	return nil
}

func (c *onDemandController) reconcile(ctx context.Context) time.Duration {
	if err := c.reconcileOnce(ctx); err != nil {
		metricOnDemandReconciliations.WithLabelValues("error").Inc()
		var rateLimit *githubv63.RateLimitError
		if errors.As(err, &rateLimit) {
			delay := max(c.pollInterval, rateLimit.Rate.Reset.Sub(c.now())+time.Second)
			c.logger.Warn().Err(err).Dur("retry_after", delay).
				Msg("GitHub API quota exhausted; delaying on-demand reconciliation")
			return delay
		}
		c.logger.Error().Err(err).Msg("On-demand reconciliation failed")
		return c.pollInterval
	}

	metricOnDemandReconciliations.WithLabelValues("success").Inc()
	return c.pollInterval
}

func (c *onDemandController) reconcileOnce(ctx context.Context) error {
	reconciledJobs := make(map[int64]*jobDemandEntry)
	assignedElsewhere := make(map[int64]struct{})

	for organization, installationID := range c.installationIDs {
		repositories, err := c.cachedRepositories(ctx, installationID)
		if err != nil {
			return fmt.Errorf("listing repositories for organization %q: %w", organization, err)
		}

		for _, repository := range repositories {
			owner := repository.GetOwner().GetLogin()
			name := repository.GetName()

			for _, status := range []string{"queued", "in_progress"} {
				runs, err := c.listWorkflowRunsFn(ctx, installationID, owner, name, status)
				if err != nil {
					return fmt.Errorf("listing workflow runs for %s/%s status %q: %w", owner, name, status, err)
				}

				for _, run := range runs {
					jobs, err := c.listWorkflowJobsFn(ctx, installationID, owner, name, run.GetID())
					if err != nil {
						return fmt.Errorf("listing workflow jobs for %s/%s run %d: %w", owner, name, run.GetID(), err)
					}

					for _, job := range jobs {
						if job == nil || job.GetID() == 0 {
							continue
						}
						if job.GetStatus() != "queued" && job.GetStatus() != "in_progress" {
							continue
						}

						poolName, matchResult := c.matchPool(organization, job.Labels)
						if matchResult != "matched" {
							continue
						}

						ownership := "queued"
						decision := "keep"
						if job.GetStatus() == "in_progress" {
							ownership = c.workflowJobRunnerOwnership(job)
							if ownership == "other" {
								decision = "drop"
								assignedElsewhere[job.GetID()] = struct{}{}
							}
						}

						c.logger.Info().
							Int64("run_id", run.GetID()).
							Int64("job_id", job.GetID()).
							Int64("runner_id", job.GetRunnerID()).
							Str("runner_name", job.GetRunnerName()).
							Str("repository", owner+"/"+name).
							Str("organization", organization).
							Str("pool", poolName).
							Str("status", job.GetStatus()).
							Str("runner_ownership", ownership).
							Str("decision", decision).
							Str("workflow_name", job.GetWorkflowName()).
							Str("job_name", job.GetName()).
							Strs("labels", job.Labels).
							Msg("Reconciled workflow job demand")

						if decision == "drop" {
							continue
						}

						reconciledJobs[job.GetID()] = &jobDemandEntry{
							organization: organization,
							poolName:     poolName,
							runner: demandRunnerMetadataFromWorkflowJob(
								poolName,
								job.GetID(),
								job.GetWorkflowName(),
								job.GetName(),
							),
						}
					}
				}
			}
		}
	}

	c.mergeReconciledJobs(reconciledJobs, assignedElsewhere, c.now())
	return nil
}

func (c *onDemandController) upsertJob(jobID int64, entry *jobDemandEntry) {
	now := c.now()

	c.mu.Lock()
	c.pruneCompletedJobsLocked(now)
	if completedAt, ok := c.completedJobs[jobID]; ok && now.Sub(completedAt) <= onDemandCompletedJobWindow {
		poolDemand := c.poolDemandLocked()
		c.mu.Unlock()
		c.applyPoolDemand(poolDemand)
		return
	}

	delete(c.completedJobs, jobID)
	entry.lastSeenAt = now
	c.jobs[jobID] = entry
	poolDemand := c.poolDemandLocked()
	c.mu.Unlock()

	c.applyPoolDemand(poolDemand)
}

func (c *onDemandController) removeJob(jobID int64) {
	now := c.now()

	c.mu.Lock()
	delete(c.jobs, jobID)
	c.completedJobs[jobID] = now
	c.pruneCompletedJobsLocked(now)
	poolDemand := c.poolDemandLocked()
	c.mu.Unlock()

	c.applyPoolDemand(poolDemand)
}

func (c *onDemandController) dropJob(jobID int64) {
	c.mu.Lock()
	delete(c.jobs, jobID)
	poolDemand := c.poolDemandLocked()
	c.mu.Unlock()

	c.applyPoolDemand(poolDemand)
}

func (c *onDemandController) mergeReconciledJobs(jobs map[int64]*jobDemandEntry, assignedElsewhere map[int64]struct{}, now time.Time) {
	c.mu.Lock()
	c.pruneCompletedJobsLocked(now)

	mergedJobs := make(map[int64]*jobDemandEntry, len(c.jobs)+len(jobs))
	for jobID, existing := range c.jobs {
		if existing == nil {
			continue
		}
		if _, ok := assignedElsewhere[jobID]; ok {
			c.logger.Info().
				Int64("job_id", jobID).
				Str("organization", existing.organization).
				Str("pool", existing.poolName).
				Str("workflow_name", existing.runner.WorkflowName).
				Str("job_name", existing.runner.JobName).
				Msg("Dropping cached workflow job demand assigned to another host")
			continue
		}
		missingFor := now.Sub(existing.lastSeenAt)
		if missingFor > onDemandMissingJobGrace {
			c.logger.Info().
				Int64("job_id", jobID).
				Dur("missing_for", missingFor).
				Str("organization", existing.organization).
				Str("pool", existing.poolName).
				Str("workflow_name", existing.runner.WorkflowName).
				Str("job_name", existing.runner.JobName).
				Msg("Expiring missing workflow job demand")
			continue
		}
		if _, ok := jobs[jobID]; !ok {
			c.logger.Info().
				Int64("job_id", jobID).
				Dur("missing_for", missingFor).
				Dur("missing_grace", onDemandMissingJobGrace).
				Str("organization", existing.organization).
				Str("pool", existing.poolName).
				Str("workflow_name", existing.runner.WorkflowName).
				Str("job_name", existing.runner.JobName).
				Msg("Preserving missing workflow job demand")
		}

		entryCopy := *existing
		mergedJobs[jobID] = &entryCopy
	}

	for jobID, job := range jobs {
		if _, ok := assignedElsewhere[jobID]; ok {
			continue
		}
		if job == nil {
			continue
		}
		if completedAt, ok := c.completedJobs[jobID]; ok && now.Sub(completedAt) <= onDemandCompletedJobWindow {
			continue
		}

		entryCopy := *job
		entryCopy.lastSeenAt = now
		mergedJobs[jobID] = &entryCopy
	}

	c.jobs = mergedJobs
	poolDemand := c.poolDemandLocked()
	c.mu.Unlock()

	c.applyPoolDemand(poolDemand)
}

func (c *onDemandController) workflowJobAssignedToAnotherHost(job *githubv63.WorkflowJob) bool {
	return c.workflowJobRunnerOwnership(job) == "other"
}

func (c *onDemandController) workflowJobRunnerOwnership(job *githubv63.WorkflowJob) string {
	if job == nil {
		return "unassigned"
	}

	runnerID := job.GetRunnerID()
	runnerName := job.GetRunnerName()
	if runnerID == 0 && runnerName == "" {
		return "unassigned"
	}

	if c.server.ownsRunner(runnerID, runnerName) {
		return "local"
	}

	return "other"
}

func (c *onDemandController) pruneCompletedJobsLocked(now time.Time) {
	for jobID, completedAt := range c.completedJobs {
		if now.Sub(completedAt) > onDemandCompletedJobWindow {
			delete(c.completedJobs, jobID)
		}
	}
}

func (c *onDemandController) poolDemandLocked() map[string]poolDemandEntry {
	poolDemand := make(map[string]poolDemandEntry)
	jobIDs := make([]int64, 0, len(c.jobs))
	for jobID := range c.jobs {
		jobIDs = append(jobIDs, jobID)
	}
	sort.Slice(jobIDs, func(i, j int) bool {
		return jobIDs[i] < jobIDs[j]
	})

	for _, jobID := range jobIDs {
		job := c.jobs[jobID]
		if job == nil {
			continue
		}
		demand := poolDemand[job.poolName]
		demand.count++
		if job.runner.Prefix != "" {
			demand.runners = append(demand.runners, job.runner)
		}
		poolDemand[job.poolName] = demand
	}

	return poolDemand
}

func (c *onDemandController) now() time.Time {
	if c.nowFn == nil {
		return time.Now().UTC()
	}

	return c.nowFn().UTC()
}

func (c *onDemandController) applyPoolDemand(poolDemand map[string]poolDemandEntry) {
	for _, pool := range c.server.snapshotPools() {
		demand := poolDemand[pool.config.Name]
		pool.SetDemandReplicasWithRunnerMetadata(demand.count, demand.runners)
		metricOnDemandJobsActive.WithLabelValues(pool.config.Name, pool.config.Runner.Organization).Set(float64(demand.count))
	}
}

func (c *onDemandController) organizationFromEvent(event *githubv63.WorkflowJobEvent) string {
	if event.GetOrg().GetLogin() != "" {
		return event.GetOrg().GetLogin()
	}

	return event.GetRepo().GetOwner().GetLogin()
}

func (c *onDemandController) matchPool(organization string, labels []string) (string, string) {
	matches := make(map[string]struct{})
	for _, label := range labels {
		pool, err := c.server.findPool(label)
		if err != nil {
			continue
		}
		if pool.config.Runner.Organization != organization {
			continue
		}

		matches[pool.config.Name] = struct{}{}
	}

	switch len(matches) {
	case 0:
		return "", "unmatched"
	case 1:
		for poolName := range matches {
			return poolName, "matched"
		}
	}

	return "", "ambiguous"
}

func (c *onDemandController) cachedRepositories(ctx context.Context, installationID int64) ([]*githubv63.Repository, error) {
	if entry, ok := c.repositoryCache[installationID]; ok && c.now().Before(entry.expiresAt) {
		return entry.repositories, nil
	}

	repositories, err := c.listRepositoriesFn(ctx, installationID)
	if err != nil {
		return nil, err
	}
	c.repositoryCache[installationID] = repositoryCacheEntry{
		repositories: repositories,
		expiresAt:    c.now().Add(onDemandRepositoryCacheTTL),
	}
	return repositories, nil
}

func (c *onDemandController) listRepositories(ctx context.Context, installationID int64) ([]*githubv63.Repository, error) {
	client := c.server.github.Installation(installationID)
	options := &githubv63.ListOptions{PerPage: 100}
	repositories := make([]*githubv63.Repository, 0)

	for {
		result, response, err := client.Apps.ListRepos(ctx, options)
		if err != nil {
			return nil, err
		}

		repositories = append(repositories, result.Repositories...)
		if response.NextPage == 0 {
			return repositories, nil
		}

		options.Page = response.NextPage
	}
}

func (c *onDemandController) listWorkflowRuns(ctx context.Context, installationID int64, owner, repo, status string) ([]*githubv63.WorkflowRun, error) {
	client := c.server.github.Installation(installationID)
	options := &githubv63.ListWorkflowRunsOptions{
		Status: status,
		ListOptions: githubv63.ListOptions{
			PerPage: 100,
		},
	}
	runs := make([]*githubv63.WorkflowRun, 0)

	for {
		result, response, err := client.Actions.ListRepositoryWorkflowRuns(ctx, owner, repo, options)
		if err != nil {
			return nil, err
		}

		runs = append(runs, result.WorkflowRuns...)
		if response.NextPage == 0 {
			return runs, nil
		}

		options.Page = response.NextPage
	}
}

func (c *onDemandController) listWorkflowJobs(ctx context.Context, installationID int64, owner, repo string, runID int64) ([]*githubv63.WorkflowJob, error) {
	client := c.server.github.Installation(installationID)
	options := &githubv63.ListWorkflowJobsOptions{
		ListOptions: githubv63.ListOptions{
			PerPage: 100,
		},
	}
	jobs := make([]*githubv63.WorkflowJob, 0)

	for {
		result, response, err := client.Actions.ListWorkflowJobs(ctx, owner, repo, runID, options)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, result.Jobs...)
		if response.NextPage == 0 {
			return jobs, nil
		}

		options.Page = response.NextPage
	}
}
