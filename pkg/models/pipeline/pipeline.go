package pipeline

import "github.com/jenkins-zh/jenkins-client/pkg/job"

// Metadata holds some of pipeline fields that are only things we needed instead of whole job.Pipeline.
type Metadata struct {
	WeatherScore                   int                       `json:"weatherScore,omitempty"`
	EstimatedDurationInMillis      int64                     `json:"estimatedDurationInMillis,omitempty"`
	Parameters                     []job.ParameterDefinition `json:"parameters,omitempty"`
	Name                           string                    `json:"name,omitempty"`
	Disabled                       bool                      `json:"disabled,omitempty"`
	NumberOfPipelines              int                       `json:"numberOfPipelines,omitempty"`
	NumberOfFolders                int                       `json:"numberOfFolders,omitempty"`
	PipelineFolderNames            []string                  `json:"pipelineFolderNames,omitempty"`
	TotalNumberOfBranches          int                       `json:"totalNumberOfBranches,omitempty"`
	NumberOfFailingBranches        int                       `json:"numberOfFailingBranches,omitempty"`
	NumberOfSuccessfulBranches     int                       `json:"numberOfSuccessfulBranches,omitempty"`
	TotalNumberOfPullRequests      int                       `json:"totalNumberOfPullRequests,omitempty"`
	NumberOfFailingPullRequests    int                       `json:"numberOfFailingPullRequests,omitempty"`
	NumberOfSuccessfulPullRequests int                       `json:"numberOfSuccessfulPullRequests,omitempty"`
	BranchNames                    []string                  `json:"branchNames,omitempty"`
	SCMSource                      *job.SCMSource            `json:"scmSource,omitempty"`
	ScriptPath                     string                    `json:"scriptPath,omitempty"`
}

// Branch contains branch metadata, like branch and pull request, and latest PipelineRun.
type Branch struct {
	Name         string           `json:"name,omitempty"`
	WeatherScore int              `json:"weatherScore,omitempty"`
	LatestRun    *LatestRun       `json:"latestRun,omitempty"`
	Branch       *job.Branch      `json:"branch,omitempty"`
	PullRequest  *job.PullRequest `json:"pullRequest,omitempty"`
}

// LatestRun contains metadata of latest PipelineRun.
type LatestRun struct {
	Causes    []Cause  `json:"causes,omitempty"`
	EndTime   job.Time `json:"endTime,omitempty"`
	StartTime job.Time `json:"startTime,omitempty"`
	ID        string   `json:"id,omitempty"`
	Name      string   `json:"name,omitempty"`
	Pipeline  string   `json:"pipeline,omitempty"`
	Result    string   `json:"result,omitempty"`
	State     string   `json:"state,omitempty"`
}

// Cause contains short description of cause.
type Cause struct {
	ShortDescription string `json:"shortDescription,omitempty"`
}
