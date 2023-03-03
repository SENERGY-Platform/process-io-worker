/*
 * Copyright (c) 2023 InfAI (CC SES)
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

package tests

import (
	"context"
	"encoding/json"
	api_configuration "github.com/SENERGY-Platform/process-io-api/pkg/configuration"
	model2 "github.com/SENERGY-Platform/process-io-api/pkg/model"
	"github.com/SENERGY-Platform/process-io-worker/pkg"
	"github.com/SENERGY-Platform/process-io-worker/pkg/configuration"
	"github.com/SENERGY-Platform/process-io-worker/pkg/model"
	"github.com/SENERGY-Platform/process-io-worker/pkg/tests/mocks"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestWorker(t *testing.T) {
	now := time.Unix(0, 0)
	backup := api_configuration.TimeNow
	defer func() { api_configuration.TimeNow = backup }()
	api_configuration.TimeNow = func() time.Time {
		return now
	}

	sources := []string{"api", "postgres", "mongodb"}

	for _, source := range sources {
		t.Run("worker "+source, testWorker(source))
	}
}

func testWorker(source string) func(t *testing.T) {
	return func(t *testing.T) {
		config, err := configuration.Load("../../config.json")
		if err != nil {
			t.Error(err)
			return
		}
		config.IncidentHandler = configuration.CamundaIncidentHandler

		if source == "api" {
			source = "mongodb"
		}

		wg := &sync.WaitGroup{}
		defer wg.Wait()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		apiconfig, apictrl, err := StartApiEnv(ctx, wg, source)
		if err != nil {
			t.Error(err)
			return
		}

		config.CamundaWorkerWaitDurationInMs = 100
		config.IoDataSource = source
		switch source {
		case configuration.PostgresDb:
			config.PostgresConnString = apiconfig.PostgresConnString
		case configuration.MongoDb:
			config.MongoUrl = apiconfig.MongoUrl
		case configuration.ApiClient:
			config.IoApiUrl = "http://localhost:" + apiconfig.ServerPort
		}

		camundamock := mocks.NewCamundaMock(ctx, []interface{}{
			[]model.CamundaExternalTask{
				{
					Id:                  "task1",
					ProcessInstanceId:   "instance1",
					ProcessDefinitionId: "definition1",
					TenantId:            "user1",
					Variables: map[string]model.CamundaVariable{
						"ignored":                {Value: float64(13)},
						config.WritePrefix + "a": {Value: "a"},
						config.WritePrefix + "instance_" + config.InstanceIdPlaceholder + "_b":            {Value: "b"},
						config.WritePrefix + "definition_" + config.ProcessDefinitionIdPlaceholder + "_c": {Value: "c"},
						config.WritePrefix + "number":  {Value: float64(42)},
						config.WritePrefix + "jsonStr": {Value: `"json-string"`},
						config.WritePrefix + "jsonObj": {Value: `{"foo": true}`},
						config.WritePrefix + "bool":    {Value: true},

						config.ReadPrefix + "with_default1":                                                  {Value: "instance_" + config.InstanceIdPlaceholder + "_with_default1"},
						config.DefaultPrefix + "instance_" + config.InstanceIdPlaceholder + "_with_default1": {Value: "defaultstringvalue1"},

						config.ReadPrefix + "with_default_number":                                                  {Value: "instance_" + config.InstanceIdPlaceholder + "_with_default_number"},
						config.DefaultPrefix + "instance_" + config.InstanceIdPlaceholder + "_with_default_number": {Value: float64(42)},

						config.ReadPrefix + "with_default_json_number":                                                  {Value: "instance_" + config.InstanceIdPlaceholder + "_with_default_json_number"},
						config.DefaultPrefix + "instance_" + config.InstanceIdPlaceholder + "_with_default_json_number": {Value: "42"},

						config.ReadPrefix + "with_default_json_str":                                                  {Value: "instance_" + config.InstanceIdPlaceholder + "_with_default_json_str"},
						config.DefaultPrefix + "instance_" + config.InstanceIdPlaceholder + "_with_default_json_str": {Value: `"defaultstringvalue"`},

						config.WritePrefix + "instance_" + config.InstanceIdPlaceholder + "_with_default2": {Value: "foobar"},

						config.ReadPrefix + "unknown": {Value: "unknown_key"},
					},
				},
			},
			[]model.CamundaExternalTask{
				{
					Id:                  "task2",
					ProcessInstanceId:   "instance1",
					ProcessDefinitionId: "definition1",
					TenantId:            "user1",
					Variables: map[string]model.CamundaVariable{
						"ignored":                       {Value: float64(13)},
						config.ReadPrefix + "o_a":       {Value: "a"},
						config.ReadPrefix + "o_b":       {Value: "instance_" + config.InstanceIdPlaceholder + "_b"},
						config.ReadPrefix + "o_c":       {Value: "definition_" + config.ProcessDefinitionIdPlaceholder + "_c"},
						config.ReadPrefix + "o_number":  {Value: "number"},
						config.ReadPrefix + "o_jsonStr": {Value: "jsonStr"},
						config.ReadPrefix + "o_jsonObj": {Value: "jsonObj"},
						config.ReadPrefix + "o_bool":    {Value: "bool"},

						config.ReadPrefix + "with_default1":                                                  {Value: "instance_" + config.InstanceIdPlaceholder + "_with_default1"},
						config.DefaultPrefix + "instance_" + config.InstanceIdPlaceholder + "_with_default1": {Value: "defaultstringvalue1"},

						config.ReadPrefix + "with_default2":                                                  {Value: "instance_" + config.InstanceIdPlaceholder + "_with_default2"},
						config.DefaultPrefix + "instance_" + config.InstanceIdPlaceholder + "_with_default2": {Value: "defaultstringvalue2"},
					},
				},
			},
		})
		if err != nil {
			t.Error(err)
			return
		}

		config.CamundaUrl = camundamock.Server.URL

		time.Sleep(200 * time.Millisecond)

		err = pkg.Start(ctx, wg, config)
		if err != nil {
			t.Error(err)
			t.Logf("%#v\n", config)
			return
		}

		time.Sleep(2 * time.Second)

		if len(camundamock.GetUnexpectedRequests()) > 0 {
			t.Error(camundamock.GetUnexpectedRequests())
			return
		}
		if len(camundamock.GetStopRequests()) > 0 {
			t.Error(camundamock.GetStopRequests())
			return
		}
		expectedCamundaCompletes := map[string][]interface{}{
			"/engine-rest/external-task/task1/complete": {
				map[string]interface{}{
					"localVariables": map[string]interface{}{
						"unknown":                  map[string]interface{}{},
						"with_default1":            map[string]interface{}{"value": "defaultstringvalue1"},
						"with_default_json_number": map[string]interface{}{"value": float64(42)},
						"with_default_json_str":    map[string]interface{}{"value": "defaultstringvalue"},
						"with_default_number":      map[string]interface{}{"value": float64(42)},
					},
					"workerId": "process_io",
				},
			},
			"/engine-rest/external-task/task2/complete": {
				map[string]interface{}{
					"localVariables": map[string]interface{}{
						"o_a":    map[string]interface{}{"value": "a"},
						"o_b":    map[string]interface{}{"value": "b"},
						"o_bool": map[string]interface{}{"value": true},
						"o_c":    map[string]interface{}{"value": "c"},
						"o_jsonObj": map[string]interface{}{
							"value": map[string]interface{}{"foo": true}},
						"o_jsonStr":     map[string]interface{}{"value": "json-string"},
						"o_number":      map[string]interface{}{"value": float64(42)},
						"with_default1": map[string]interface{}{"value": "defaultstringvalue1"},
						"with_default2": map[string]interface{}{"value": "foobar"},
					},
					"workerId": "process_io",
				},
			},
		}
		actual := camundamock.GetCompleteRequests()
		if !reflect.DeepEqual(actual, expectedCamundaCompletes) {
			t.Errorf("\n%#v\n%#v", expectedCamundaCompletes, actual)
		}

		variables, err := apictrl.List("user1", model2.VariablesQueryOptions{})
		if err != nil {
			t.Error(err)
			return
		}

		expectedVariables := []model.VariableWithUnixTimestamp{
			{Variable: model.Variable{Key: "instance_instance1_with_default2", Value: "foobar", ProcessDefinitionId: "definition1", ProcessInstanceId: "instance1"}, UnixTimestampInS: 0},
			{Variable: model.Variable{Key: "instance_instance1_b", Value: "b", ProcessDefinitionId: "definition1", ProcessInstanceId: "instance1"}, UnixTimestampInS: 0},
			{Variable: model.Variable{Key: "jsonStr", Value: "json-string", ProcessDefinitionId: "", ProcessInstanceId: ""}, UnixTimestampInS: 0},
			{Variable: model.Variable{Key: "definition_definition1_c", Value: "c", ProcessDefinitionId: "definition1", ProcessInstanceId: ""}, UnixTimestampInS: 0},
			{Variable: model.Variable{Key: "bool", Value: true, ProcessDefinitionId: "", ProcessInstanceId: ""}, UnixTimestampInS: 0},
			{Variable: model.Variable{Key: "number", Value: float64(42), ProcessDefinitionId: "", ProcessInstanceId: ""}, UnixTimestampInS: 0},
			{Variable: model.Variable{Key: "jsonObj", Value: map[string]interface{}{"foo": true}, ProcessDefinitionId: "", ProcessInstanceId: ""}, UnixTimestampInS: 0},
			{Variable: model.Variable{Key: "a", Value: "a", ProcessDefinitionId: "", ProcessInstanceId: ""}, UnixTimestampInS: 0},
		}
		sort.Slice(expectedVariables, func(i, j int) bool {
			return expectedVariables[i].Variable.Key < expectedVariables[j].Variable.Key
		})
		sort.Slice(variables, func(i, j int) bool {
			return variables[i].Variable.Key < variables[j].Variable.Key
		})
		if !reflect.DeepEqual(jsonNormalize(variables), jsonNormalize(expectedVariables)) {
			t.Errorf("\n%#v\n%#v", expectedVariables, variables)
			return
		}
	}
}

func jsonNormalize(in interface{}) (out interface{}) {
	temp, _ := json.Marshal(in)
	json.Unmarshal(temp, &out)
	return
}
