package pipeline

import (
	"github.com/jenkins-zh/jenkins-client/pkg/job"
	"kubesphere.io/devops/pkg/models/pipeline"
)

type branchPredicate func(pipeline.Branch) bool

type branchSlice []pipeline.Branch

func (branches branchSlice) filter(predicate branchPredicate) []pipeline.Branch {
	resultBranches := []pipeline.Branch{}
	for _, branch := range branches {
		if predicate != nil && predicate(branch) {
			resultBranches = append(resultBranches, branch)
		}
	}
	return resultBranches
}

func filterBranches(branches []pipeline.Branch, filter job.Filter) []pipeline.Branch {
	var predicate branchPredicate
	switch filter {
	case job.PullRequestFilter:
		predicate = func(branch pipeline.Branch) bool {
			return branch.PullRequest != nil && branch.PullRequest.ID != ""
		}
	case job.OriginFilter:
		predicate = func(branch pipeline.Branch) bool {
			return branch.PullRequest == nil || branch.PullRequest.ID == ""
		}
	default:
		predicate = func(pb pipeline.Branch) bool {
			return true
		}
	}
	return branchSlice(branches).filter(predicate)
}
