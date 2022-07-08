/*
Copyright 2022 The KubeSphere Authors.

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

package fluxcd

import (
	"context"
	"fmt"
	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"kubesphere.io/devops/pkg/api/devops/v1alpha3"
	"kubesphere.io/devops/pkg/utils/k8sutil"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

//+kubebuilder:rbac:groups=devops.kubesphere.io,resources=gitrepositories,verbs=get;list;watch;update;delete
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// GitRepositoryReconciler is the reconciler of the FluxCDGitRepository
type GitRepositoryReconciler struct {
	client.Client
	log      logr.Logger
	recorder record.EventRecorder
}

// Reconcile maintains the FluxCDGitRepository against to the GitRepository
func (r *GitRepositoryReconciler) Reconcile(req ctrl.Request) (result ctrl.Result, err error) {
	ctx := context.Background()
	repo := &v1alpha3.GitRepository{}

	if err = r.Get(ctx, req.NamespacedName, repo); err != nil {
		err = client.IgnoreNotFound(err)
		return
	}

	err = r.reconcileFluxGitRepo(repo)
	return
}

func (r *GitRepositoryReconciler) reconcileFluxGitRepo(repo *v1alpha3.GitRepository) (err error) {
	ctx := context.Background()
	FluxGitRepo := createBareFluxGitRepoObject()
	// FluxGitRepo's namespace = v1alpha3.GitRepository's namespace
	// FluxGitRepo's name = v1alpha3.GitRepository's name + "-flux-git-repo"
	ns, name := repo.GetNamespace(), getFluxRepoName(repo.GetName())

	if !repo.ObjectMeta.DeletionTimestamp.IsZero() {
		if err = r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, FluxGitRepo); err != nil {
			err = client.IgnoreNotFound(err)
		} else {
			r.log.Info("delete FluxCDGitRepository", "name", FluxGitRepo.GetName())
			err = r.Delete(ctx, FluxGitRepo)
		}

		if err == nil {
			k8sutil.RemoveFinalizer(&repo.ObjectMeta, v1alpha3.GitRepoFinalizerName)
			err = r.Update(context.TODO(), repo)
		}
		return
	}
	if k8sutil.AddFinalizer(&repo.ObjectMeta, v1alpha3.GitRepoFinalizerName) {
		err = r.Update(context.TODO(), repo)
		if err != nil {
			return
		}
	}

	if err = r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, FluxGitRepo); err != nil {
		if !apierrors.IsNotFound(err) {
			return
		}
		// flux git repo did not existed
		// create
		if FluxGitRepo, err = r.createUnstructuredFluxGitRepo(repo); err != nil {
			return
		}
		if err = r.Create(ctx, FluxGitRepo); err != nil {
			data, _ := FluxGitRepo.MarshalJSON()
			r.log.Error(err, "failed to create FluxCDGitRepository", "data", string(data))
			r.recorder.Eventf(FluxGitRepo, v1.EventTypeWarning, "FailedWithFluxCD",
				"failed to create FluxCDGitRepository, error is: %v", err)
		}
	} else {
		// flux git repo existed
		// update
		var newFluxGitRepo *unstructured.Unstructured
		if newFluxGitRepo, err = r.createUnstructuredFluxGitRepo(repo); err == nil {
			FluxGitRepo.Object["spec"] = newFluxGitRepo.Object["spec"]
			err = retry.RetryOnConflict(retry.DefaultRetry, func() (err error) {
				latestGitRepo := createBareFluxGitRepoObject()
				if err = r.Get(context.Background(), types.NamespacedName{
					Namespace: FluxGitRepo.GetNamespace(),
					Name:      FluxGitRepo.GetName(),
				}, latestGitRepo); err != nil {
					return
				}

				FluxGitRepo.SetResourceVersion(latestGitRepo.GetResourceVersion())
				r.log.Info("update FluxCDGitRepository", "name", FluxGitRepo.GetName())
				err = r.Update(ctx, FluxGitRepo)
				return
			})
		}
	}
	return
}

func (r *GitRepositoryReconciler) createUnstructuredFluxGitRepo(repo *v1alpha3.GitRepository) (*unstructured.Unstructured, error) {
	newFluxGitRepo := createBareFluxGitRepoObject()
	newFluxGitRepo.SetNamespace(repo.GetNamespace())
	newFluxGitRepo.SetName(getFluxRepoName(repo.GetName()))

	// set url
	if err := unstructured.SetNestedField(newFluxGitRepo.Object, repo.Spec.URL, "spec", "url"); err != nil {
		return nil, err
	}
	// set interval
	if err := unstructured.SetNestedField(newFluxGitRepo.Object, "1m", "spec", "interval"); err != nil {
		return nil, err
	}
	//TODO set secretRef
	//if err := unstructured.SetNestedField(newFluxGitRepo.Object, repo.Spec.Secret.Name, "spec", "secretRef"); err != nil {
	//	return nil, err
	//}

	return newFluxGitRepo, nil
}

func getFluxRepoName(name string) string {
	return fmt.Sprintf("%s-flux-git-repo", name)
}

// GetName returns the name of this reconciler
func (r *GitRepositoryReconciler) GetName() string {
	return "FluxGitRepositoryReconciler"
}

func createBareFluxGitRepoObject() *unstructured.Unstructured {
	FluxGitRepo := &unstructured.Unstructured{}
	FluxGitRepo.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "source.toolkit.fluxcd.io",
		Version: "v1beta2",
		Kind:    "GitRepository",
	})
	return FluxGitRepo
}

// SetupWithManager setups the reconciler with a manager
// setup the logger, recorder
func (r *GitRepositoryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.log = ctrl.Log.WithName(r.GetName())
	r.recorder = mgr.GetEventRecorderFor(r.GetName())
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha3.GitRepository{}).
		Complete(r)
}
