/*
 * Copyright 2021 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mocks

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
)

type CamundaMock struct {
	fetchResponses     []interface{}
	fetchRequestCount  int
	CompleteRequests   map[string][]interface{}
	StopRequests       map[string][]interface{}
	UnexpectedRequests map[string][]interface{}
	Server             *httptest.Server
	mux                sync.Mutex
}

func NewCamundaMock(ctx context.Context, fetchResponses []interface{}) (mock *CamundaMock) {
	mock = &CamundaMock{
		mux:                sync.Mutex{},
		fetchResponses:     fetchResponses,
		fetchRequestCount:  0,
		CompleteRequests:   map[string][]interface{}{},
		StopRequests:       map[string][]interface{}{},
		UnexpectedRequests: map[string][]interface{}{},
	}
	mock.Start(ctx)
	return
}

func (this *CamundaMock) Start(ctx context.Context) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		this.mux.Lock()
		defer this.mux.Unlock()
		path := request.URL.Path
		var msg interface{}
		json.NewDecoder(request.Body).Decode(&msg)
		// DELETE /engine-rest/process-instance/"+url.PathEscape(id)+"?skipIoMappings=true
		// POST /engine-rest/external-task/"+taskInfo.TaskId+"/complete
		// POST /engine-rest/external-task/fetchAndLock
		if strings.HasPrefix(path, "/engine-rest/process-instance") {
			if request.Method == "DELETE" {
				this.StopRequests[path] = append(this.StopRequests[path], msg)
				json.NewEncoder(writer).Encode(nil)
			} else {
				this.UnexpectedRequests[path] = append(this.UnexpectedRequests[path], msg)
				http.Error(writer, "unexperequest", 500)
			}
			return
		}
		if strings.HasPrefix(path, "/engine-rest/external-task/fetchAndLock") {
			if request.Method == "POST" {
				if this.fetchRequestCount >= len(this.fetchResponses) {
					json.NewEncoder(writer).Encode([]string{})
					return
				}
				json.NewEncoder(writer).Encode(this.fetchResponses[this.fetchRequestCount])
				this.fetchRequestCount = this.fetchRequestCount + 1
				return
			} else {
				this.UnexpectedRequests[path] = append(this.UnexpectedRequests[path], msg)
				http.Error(writer, "unexperequest", 500)
			}
			return
		}
		if strings.HasPrefix(path, "/engine-rest/external-task") {
			if request.Method == "POST" {
				this.CompleteRequests[path] = append(this.CompleteRequests[path], msg)
				json.NewEncoder(writer).Encode(nil)
			} else {
				this.UnexpectedRequests[path] = append(this.UnexpectedRequests[path], msg)
				http.Error(writer, "unexperequest", 500)
			}
			return
		}

		this.UnexpectedRequests[path] = append(this.UnexpectedRequests[path], msg)
		http.Error(writer, "unexperequest", 500)
	}))
	go func() {
		<-ctx.Done()
		server.Close()
	}()
	this.Server = server
	return
}
