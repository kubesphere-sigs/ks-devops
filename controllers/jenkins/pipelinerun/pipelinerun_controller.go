/*
Copyright 2020 The KubeSphere Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pipelinerun

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/jenkins-zh/jenkins-client/pkg/core"
	"github.com/jenkins-zh/jenkins-client/pkg/job"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog"
	"kubesphere.io/devops/pkg/api/devops/v1alpha3"
	devopsClient "kubesphere.io/devops/pkg/client/devops"
	"kubesphere.io/devops/pkg/utils/sliceutil"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Reconciler reconciles a PipelineRun object
type Reconciler struct {
	client.Client
	log          logr.Logger
	Scheme       *runtime.Scheme
	DevOpsClient devopsClient.Interface
	JenkinsCore  core.JenkinsCore
	recorder     record.EventRecorder
}

//+kubebuilder:rbac:groups=devops.kubesphere.io,resources=pipelineruns,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=devops.kubesphere.io,resources=pipelineruns/status,verbs=get;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *Reconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	ctx := context.Background()
	log := r.log.WithValues("PipelineRun", req.NamespacedName)

	// get PipelineRun
	pipelineRun := &v1alpha3.PipelineRun{}
	var err error
	if err = r.Client.Get(ctx, req.NamespacedName, pipelineRun); err != nil {
		log.Error(err, "unable to fetch PipelineRun")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// don't modify the cache in other places, like informer cache.
	pipelineRunCopied := pipelineRun.DeepCopy()

	// DeletionTimestamp.IsZero() means copyPipeline has not been deleted.
	if !pipelineRunCopied.ObjectMeta.DeletionTimestamp.IsZero() {
		if err = r.deleteJenkinsJobHistory(pipelineRunCopied); err != nil {
			klog.V(4).Infof("failed to delete Jenkins job history from PipelineRun: %s/%s, error: %v",
				pipelineRunCopied.Namespace, pipelineRunCopied.Name, err)
		} else {
			pipelineRunCopied.ObjectMeta.Finalizers = sliceutil.RemoveString(pipelineRunCopied.ObjectMeta.Finalizers, func(item string) bool {
				return item == v1alpha3.PipelineRunFinalizerName
			})
			err = r.Update(context.TODO(), pipelineRunCopied)
		}
		return ctrl.Result{}, err
	}

	// the PipelineRun cannot allow building
	if !pipelineRunCopied.Buildable() {
		return ctrl.Result{}, nil
	}

	// check PipelineRef
	if pipelineRunCopied.Spec.PipelineRef == nil || pipelineRunCopied.Spec.PipelineRef.Name == "" {
		// make the PipelineRun as orphan
		return ctrl.Result{}, r.makePipelineRunOrphan(ctx, pipelineRunCopied)
	}

	// get pipeline
	pipeline := &v1alpha3.Pipeline{}
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: pipelineRunCopied.Namespace, Name: pipelineRunCopied.Spec.PipelineRef.Name}, pipeline); err != nil {
		log.Error(err, "unable to get pipeline")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	namespaceName := pipeline.Namespace
	pipelineName := pipeline.GetName()

	// set Pipeline name and SCM ref name into labels
	if pipelineRunCopied.Labels == nil {
		pipelineRunCopied.Labels = make(map[string]string)
	}
	pipelineRunCopied.Labels[v1alpha3.PipelineNameLabelKey] = pipelineName
	if refName, err := getSCMRefName(&pipelineRunCopied.Spec); err == nil && refName != "" {
		pipelineRunCopied.Labels[v1alpha3.SCMRefNameLabelKey] = refName
	}

	log = log.WithValues("namespace", namespaceName, "Pipeline", pipelineName)

	// check PipelineRun status
	if pipelineRunCopied.HasStarted() {
		log.V(5).Info("pipeline has already started, and we are retrieving run data from Jenkins.")
		pipelineBuild, err := r.getPipelineRunResult(namespaceName, pipelineName, pipelineRunCopied)
		if err != nil {
			log.Error(err, "unable get PipelineRun data.")
			r.recorder.Eventf(pipelineRunCopied, corev1.EventTypeWarning, v1alpha3.RetrieveFailed, "Failed to retrieve running data from Jenkins, and error was %s", err)
			return ctrl.Result{}, err
		}

		prNodes, err := r.getPipelineNodes(namespaceName, pipelineName, pipelineRunCopied)
		if err != nil {
			log.Error(err, "unable to get PipelineRun nodes detail")
			r.recorder.Eventf(pipelineRunCopied, corev1.EventTypeWarning, v1alpha3.RetrieveFailed, "Failed to retrieve nodes detail from Jenkins, and error was %s", err)
			return ctrl.Result{}, err
		}

		// set the latest run result into annotations
		runResultJSON, err := json.Marshal(pipelineBuild)
		if err != nil {
			return ctrl.Result{}, err
		}
		prNodesJSON, err := json.Marshal(prNodes)
		if err != nil {
			return ctrl.Result{}, err
		}
		if pipelineRunCopied.Annotations == nil {
			pipelineRunCopied.Annotations = make(map[string]string)
		}
		pipelineRunCopied.Annotations[v1alpha3.JenkinsPipelineRunStatusKey] = string(runResultJSON)
		pipelineRunCopied.Annotations[v1alpha3.JenkinsPipelineRunStagesStatusKey] = string(prNodesJSON)

		// update PipelineRun
		if err := r.updateLabelsAndAnnotations(ctx, pipelineRunCopied); err != nil {
			log.Error(err, "unable to update PipelineRun labels and annotations.")
			return ctrl.Result{RequeueAfter: time.Second}, err
		}

		status := pipelineRunCopied.Status.DeepCopy()
		pbApplier := pipelineBuildApplier{pipelineBuild}
		pbApplier.apply(status)

		// Because the status is a subresource of PipelineRun, we have to update status separately.
		// See also: https://book-v1.book.kubebuilder.io/basics/status_subresource.html
		if err := r.updateStatus(ctx, status, req.NamespacedName); err != nil {
			log.Error(err, "unable to update PipelineRun status.")
			return ctrl.Result{RequeueAfter: time.Second}, err
		}
		r.recorder.Eventf(pipelineRunCopied, corev1.EventTypeNormal, v1alpha3.Updated, "Updated running data for PipelineRun %s", req.NamespacedName)
		// until the status is okay
		// TODO make the RequeueAfter configurable
		return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
	}

	// first run
	jobRun, err := r.triggerJenkinsJob(namespaceName, pipelineName, &pipelineRunCopied.Spec)
	if err != nil {
		log.Error(err, "unable to run pipeline", "namespace", namespaceName, "pipeline", pipeline.Name)
		r.recorder.Eventf(pipelineRunCopied, corev1.EventTypeWarning, v1alpha3.TriggerFailed, "Failed to trigger PipelineRun %s, and error was %s", req.NamespacedName, err)
		return ctrl.Result{}, err
	}
	// check if there is still a same pending PipelineRun
	exists, err := r.hasSamePendingPipelineRun(jobRun, pipeline)
	if err != nil {
		log.Error(err, "unable to check if there still has the same pending PipelineRun", "jobRun", jobRun)
		return ctrl.Result{}, err
	}
	if exists {
		// if there still exists the same pending PipelineRun, then give up reconciling
		if err := r.deletePipelineRun(req.NamespacedName); err != nil {
			log.Error(err, "unable to delete PipelineRun")
			return ctrl.Result{}, err
		}
		log.Info("Skipped this PipelineRun because there was still a pending Pipeline with the same parameter")
		return ctrl.Result{}, nil
	}

	log.Info("Triggered a PipelineRun", "runID", jobRun.ID)

	// set Jenkins run ID
	if pipelineRunCopied.Annotations == nil {
		pipelineRunCopied.Annotations = make(map[string]string)
	}
	pipelineRunCopied.Annotations[v1alpha3.JenkinsPipelineRunIDKey] = jobRun.ID

	// the Update method only updates fields except subresource: status
	if err := r.updateLabelsAndAnnotations(ctx, pipelineRunCopied); err != nil {
		log.Error(err, "unable to update PipelineRun labels and annotations.")
		return ctrl.Result{}, err
	}

	pipelineRunCopied.Status.StartTime = &v1.Time{Time: time.Now()}
	pipelineRunCopied.Status.UpdateTime = &v1.Time{Time: time.Now()}
	// due to the status is subresource of PipelineRun, we have to update status separately.
	// see also: https://book-v1.book.kubebuilder.io/basics/status_subresource.html

	if err := r.updateStatus(ctx, &pipelineRunCopied.Status, req.NamespacedName); err != nil {
		log.Error(err, "unable to update PipelineRun status.")
		return ctrl.Result{}, err
	}
	r.recorder.Eventf(pipelineRunCopied, corev1.EventTypeNormal, v1alpha3.Started, "Started PipelineRun %s", req.NamespacedName)
	// requeue after 1 second
	return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
}

// hasSamePendingPipelineRun checks if there is still a PipelineRun with the same run ID.
func (r *Reconciler) hasSamePendingPipelineRun(jobRun *job.PipelineRun, pipeline *v1alpha3.Pipeline) (bool, error) {
	// check if the run ID exists in the PipelineRun
	pipelineRuns := &v1alpha3.PipelineRunList{}
	listOptions := []client.ListOption{
		client.InNamespace(pipeline.Namespace),
		client.MatchingLabels{v1alpha3.PipelineNameLabelKey: pipeline.Name},
		client.MatchingLabels{v1alpha3.JenkinsPipelineRunIDKey: jobRun.ID},
	}
	if pipeline.Spec.Type == v1alpha3.MultiBranchPipelineType {
		// add SCM reference name into list options for multi-branch Pipeline
		listOptions = append(listOptions, client.MatchingLabels{v1alpha3.SCMRefNameLabelKey: jobRun.Pipeline})
	}
	if err := r.Client.List(context.Background(), pipelineRuns, listOptions...); err != nil {
		return false, err
	}
	if len(pipelineRuns.Items) == 0 {
		return false, nil
	}
	return true, nil
}

// deletePipelineRun deletes PipelineRun by namespace and name.
func (r *Reconciler) deletePipelineRun(pipelineRunKey client.ObjectKey) error {
	pipelineRunToDelete := &v1alpha3.PipelineRun{}
	if err := r.Client.Get(context.Background(), pipelineRunKey, pipelineRunToDelete); err != nil {
		return err
	}
	return r.Client.Delete(context.Background(), pipelineRunToDelete)
}

func (r *Reconciler) deleteJenkinsJobHistory(pipelineRun *v1alpha3.PipelineRun) (err error) {
	var buildNum int
	if buildNum = getJenkinsBuildNumber(pipelineRun); buildNum < 0 {
		return
	}

	jenkinsClient := job.Client{
		JenkinsCore: r.JenkinsCore,
	}
	jobName := fmt.Sprintf("job/%s/job/%s", pipelineRun.Namespace, pipelineRun.Spec.PipelineRef.Name)
	if err = jenkinsClient.DeleteHistory(jobName, buildNum); err != nil {
		// TODO improve the way to check if the desired build record was deleted
		if strings.Contains(err.Error(), "not found resources") {
			err = nil
		} else {
			err = fmt.Errorf("failed to delete Jenkins job: %s, build: %d, error: %v", jobName, buildNum, err)
		}
	}
	return
}

// getJenkinsBuildNumber returns the build number of a Jenkins job build which related with a PipelineRun
// return a negative value if there is no valid build number
func getJenkinsBuildNumber(pipelineRun *v1alpha3.PipelineRun) (num int) {
	num = -1

	var (
		buildNum      string
		buildNumExist bool
	)

	if buildNum, buildNumExist = pipelineRun.GetPipelineRunID(); !buildNumExist {
		return
	}

	var err error
	if num, err = strconv.Atoi(buildNum); err != nil {
		num = -1
		klog.V(7).Infof("found an invalid build number from PipelineRun: %s/%s, err: %v",
			pipelineRun.Namespace, pipelineRun.Name, err)
	}
	return
}

func (r *Reconciler) triggerJenkinsJob(devopsProjectName, pipelineName string, prSpec *v1alpha3.PipelineRunSpec) (*job.PipelineRun, error) {
	c := job.BlueOceanClient{JenkinsCore: r.JenkinsCore, Organization: "jenkins"}

	branch, err := getSCMRefName(prSpec)
	if err != nil {
		return nil, err
	}

	return c.Build(job.BuildOption{
		Pipelines:  []string{devopsProjectName, pipelineName},
		Parameters: parameterConverter{parameters: prSpec.Parameters}.convert(),
		Branch:     branch,
	})
}

func getSCMRefName(prSpec *v1alpha3.PipelineRunSpec) (string, error) {
	var branch = ""
	if prSpec.IsMultiBranchPipeline() {
		if prSpec.SCM == nil || prSpec.SCM.RefName == "" {
			return "", fmt.Errorf("failed to obtain SCM reference name for multi-branch Pipeline")
		}
		branch = prSpec.SCM.RefName
	}
	return branch, nil
}

func (r *Reconciler) getPipelineRunResult(devopsProjectName, pipelineName string, pr *v1alpha3.PipelineRun) (*job.PipelineRun, error) {
	runID, exists := pr.GetPipelineRunID()
	if !exists {
		return nil, fmt.Errorf("unable to get PipelineRun result due to not found run ID")
	}
	c := job.BlueOceanClient{JenkinsCore: r.JenkinsCore, Organization: "jenkins"}

	branch, err := getSCMRefName(&pr.Spec)
	if err != nil {
		return nil, err
	}
	return c.GetBuild(job.GetBuildOption{
		RunID:     runID,
		Pipelines: []string{devopsProjectName, pipelineName},
		Branch:    branch,
	})
}

func (r *Reconciler) getPipelineNodes(devopsProjectName, pipelineName string, pr *v1alpha3.PipelineRun) ([]job.Node, error) {
	runID, exists := pr.GetPipelineRunID()
	if !exists {
		return nil, fmt.Errorf("unable to get PipelineRun result due to not found run ID")
	}
	c := job.BlueOceanClient{JenkinsCore: r.JenkinsCore, Organization: "jenkins"}
	branch, err := getSCMRefName(&pr.Spec)
	if err != nil {
		return nil, err
	}
	return c.GetNodes(job.GetNodesOption{
		Pipelines: []string{devopsProjectName, pipelineName},
		Branch:    branch,
		RunID:     runID,
	})
}

func (r *Reconciler) updateLabelsAndAnnotations(ctx context.Context, pr *v1alpha3.PipelineRun) error {
	// get pipeline
	prToUpdate := v1alpha3.PipelineRun{}
	err := r.Get(ctx, client.ObjectKey{Namespace: pr.Namespace, Name: pr.Name}, &prToUpdate)
	if err != nil {
		return err
	}
	if reflect.DeepEqual(pr.Labels, prToUpdate.Labels) && reflect.DeepEqual(pr.Annotations, prToUpdate.Annotations) {
		return nil
	}
	prToUpdate = *prToUpdate.DeepCopy()
	prToUpdate.Labels = pr.Labels
	prToUpdate.Annotations = pr.Annotations
	// make sure all PipelineRuns have the finalizer
	if !sliceutil.HasString(prToUpdate.ObjectMeta.Finalizers, v1alpha3.PipelineRunFinalizerName) {
		prToUpdate.ObjectMeta.Finalizers = append(prToUpdate.ObjectMeta.Finalizers, v1alpha3.PipelineRunFinalizerName)
	}
	return r.Update(ctx, &prToUpdate)
}

func (r *Reconciler) updateStatus(ctx context.Context, desiredStatus *v1alpha3.PipelineRunStatus, prKey client.ObjectKey) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		prToUpdate := v1alpha3.PipelineRun{}
		err := r.Get(ctx, prKey, &prToUpdate)
		if err != nil {
			return err
		}
		if reflect.DeepEqual(*desiredStatus, prToUpdate.Status) {
			return nil
		}
		prToUpdate = *prToUpdate.DeepCopy()
		prToUpdate.Status = *desiredStatus
		return r.Status().Update(ctx, &prToUpdate)
	})
}

func (r *Reconciler) makePipelineRunOrphan(ctx context.Context, pr *v1alpha3.PipelineRun) error {
	// make the PipelineRun as orphan
	prToUpdate := pr.DeepCopy()
	prToUpdate.LabelAsAnOrphan()
	if err := r.updateLabelsAndAnnotations(ctx, prToUpdate); err != nil {
		return err
	}
	condition := v1alpha3.Condition{
		Type:               v1alpha3.ConditionSucceeded,
		Status:             v1alpha3.ConditionUnknown,
		Reason:             "SKIPPED",
		Message:            "skipped to reconcile this PipelineRun due to not found Pipeline reference in PipelineRun.",
		LastTransitionTime: v1.Now(),
		LastProbeTime:      v1.Now(),
	}
	prToUpdate.Status.AddCondition(&condition)
	prToUpdate.Status.Phase = v1alpha3.Unknown
	return r.updateStatus(ctx, &pr.Status, client.ObjectKey{Namespace: pr.Namespace, Name: pr.Name})
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	// the name should obey Kubernetes naming convention: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
	r.recorder = mgr.GetEventRecorderFor("pipelinerun-controller")
	r.log = ctrl.Log.WithName("pipelinerun-controller")
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha3.PipelineRun{}).
		Complete(r)
}
