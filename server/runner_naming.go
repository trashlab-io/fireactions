package server

import (
	"fmt"
	"strings"
	"unicode"
)

type demandRunnerMetadata struct {
	Prefix       string
	WorkflowName string
	JobName      string
}

func demandRunnerMetadataFromWorkflowJob(poolName string, jobID int64, workflowName, jobName string) demandRunnerMetadata {
	prefixParts := []string{poolName}
	if workflowName != "" {
		prefixParts = append(prefixParts, workflowName)
	}
	if jobName != "" && jobName != workflowName {
		prefixParts = append(prefixParts, jobName)
	}
	if jobID != 0 {
		prefixParts = append(prefixParts, fmt.Sprintf("%d", jobID))
	}

	prefix := sanitizeRunnerName(strings.Join(prefixParts, "-"))
	if prefix == "" {
		prefix = sanitizeRunnerName(poolName)
	}

	return demandRunnerMetadata{
		Prefix:       prefix,
		WorkflowName: workflowName,
		JobName:      jobName,
	}
}

func sanitizeRunnerName(value string) string {
	var b strings.Builder
	previousHyphen := false

	for _, r := range strings.ToLower(value) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			previousHyphen = false
			continue
		}

		if !previousHyphen {
			b.WriteByte('-')
			previousHyphen = true
		}
	}

	return strings.Trim(b.String(), "-")
}
