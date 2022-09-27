// Copyright 2022 KubeSphere Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package argocd

import (
	"bytes"
	"encoding/json"
	"github.com/emicklei/go-restful"
	"github.com/stretchr/testify/assert"
	"io"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/kubernetes/scheme"
	"kubesphere.io/devops/pkg/api/gitops/v1alpha1"
	"kubesphere.io/devops/pkg/apiserver/request"
	"kubesphere.io/devops/pkg/kapis/common"
	"net/http"
	"net/http/httptest"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

func Test_handler_handleSyncApplication(t *testing.T) {
	createApp := func(name string, op *v1alpha1.Operation) *v1alpha1.Application {
		return &v1alpha1.Application{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "fake-namespace",
			},
			Spec: v1alpha1.ApplicationSpec{
				ArgoApp: &v1alpha1.ArgoApplication{
					Operation: op,
				},
			},
		}
	}
	createRequest := func(name string, syncRequest *ApplicationSyncRequest, withUser bool) *restful.Request {
		var body io.Reader
		if syncRequest != nil {
			bodyJSON, err := json.Marshal(syncRequest)
			assert.NoError(t, err)
			body = bytes.NewBuffer(bodyJSON)
		}
		testReq := httptest.NewRequest(http.MethodPost, "/applications/app/sync", body)
		testReq.Header.Set(restful.HEADER_ContentType, restful.MIME_JSON)
		if withUser {
			ctx := request.WithUser(testReq.Context(), &user.DefaultInfo{
				Name: "fake-user",
			})
			testReq = testReq.WithContext(ctx)
		}
		req := restful.NewRequest(testReq)
		req.PathParameters()[common.NamespacePathParameter.Data().Name] = "fake-namespace"
		req.PathParameters()[pathParameterApplication.Data().Name] = name
		return req
	}
	type fields struct {
		apps []v1alpha1.Application
	}
	type args struct {
		req *restful.Request
	}
	tests := []struct {
		name             string
		fields           fields
		args             args
		wantResponseCode int
		verifyResponse   func(t *testing.T, response string)
	}{{
		name: "Should return bad request error if sync request is nil",
		fields: fields{
			apps: []v1alpha1.Application{
				*createApp("fake-app", nil),
			},
		},
		args: args{
			req: createRequest("fake-app", nil, true),
		},
		wantResponseCode: http.StatusBadRequest,
		verifyResponse: func(t *testing.T, response string) {
			assert.Contains(t, response, invalidRequestBodyError.Error())
		},
	}, {
		name: "Should return 404 if app is not found",
		fields: fields{
			apps: []v1alpha1.Application{
				*createApp("fake-app", nil),
			},
		},
		args: args{
			req: createRequest("another-fake-app", &ApplicationSyncRequest{}, true),
		},
		wantResponseCode: http.StatusNotFound,
		verifyResponse: func(t *testing.T, response string) {
			assert.Contains(t, response, "applications.gitops.kubesphere.io \"another-fake-app\" not found")
		},
	}, {
		name: "Should return 401 if unauthenticated user requests this endpoint",
		fields: fields{
			apps: []v1alpha1.Application{
				*createApp("fake-app", nil),
			},
		},
		args: args{
			req: createRequest("fake-app", &ApplicationSyncRequest{}, false),
		},
		wantResponseCode: http.StatusUnauthorized,
		verifyResponse: func(t *testing.T, response string) {
			assert.Contains(t, response, unauthenticatedError.Error())
		},
	}, {
		name: "Should return 400 if argoApp is not initialized",
		fields: fields{
			apps: []v1alpha1.Application{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "fake-app",
						Namespace: "fake-namespace",
					},
					Spec: v1alpha1.ApplicationSpec{
						ArgoApp: nil,
					},
				},
			},
		},
		args: args{
			req: createRequest("fake-app", &ApplicationSyncRequest{}, true),
		},
		wantResponseCode: http.StatusBadRequest,
		verifyResponse: func(t *testing.T, response string) {
			assert.Contains(t, response, argoAppNotConfiguredError.Error())
		},
	}, {
		name: "Should update operation field if app has no operation field",
		fields: fields{
			apps: []v1alpha1.Application{
				*createApp("fake-app", nil),
			},
		},
		args: args{
			req: createRequest("fake-app", &ApplicationSyncRequest{
				Prune:  true,
				DryRun: true,
				RetryStrategy: &v1alpha1.RetryStrategy{
					Limit: 10,
				},
				Strategy: &v1alpha1.SyncStrategy{
					Apply: &v1alpha1.SyncStrategyApply{
						Force: true,
					},
				},
				SyncOptions: &v1alpha1.SyncOptions{"fake-option=true"},
			}, true),
		},
		wantResponseCode: http.StatusOK,
		verifyResponse: func(t *testing.T, response string) {
			gotApp := &v1alpha1.Application{}
			err := json.Unmarshal([]byte(response), gotApp)
			assert.NoError(t, err)
			assert.NotNil(t, gotApp.Spec.ArgoApp.Operation)
			gotOp := gotApp.Spec.ArgoApp.Operation
			assert.True(t, gotOp.Sync.Prune)
			assert.True(t, gotOp.Sync.DryRun)
			assert.Equal(t, int64(10), gotOp.Retry.Limit)
			assert.True(t, gotOp.Sync.SyncStrategy.Apply.Force)
			assert.Equal(t, v1alpha1.SyncOptions{"fake-option=true"}, gotOp.Sync.SyncOptions)
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			utilruntime.Must(v1alpha1.AddToScheme(scheme.Scheme))
			fakeClient := fake.NewFakeClientWithScheme(scheme.Scheme, toObjects(tt.fields.apps)...)
			h := &handler{
				Client: fakeClient,
			}

			recorder := httptest.NewRecorder()
			resp := restful.NewResponse(recorder)
			resp.SetRequestAccepts(restful.MIME_JSON)
			// test entrypoint
			h.handleSyncApplication(tt.args.req, resp)
			assert.Equal(t, tt.wantResponseCode, recorder.Code)
			tt.verifyResponse(t, recorder.Body.String())
		})
	}
}

func toObjects(apps []v1alpha1.Application) []runtime.Object {
	objs := make([]runtime.Object, len(apps))
	for i := range apps {
		objs[i] = &apps[i]
	}
	return objs
}
