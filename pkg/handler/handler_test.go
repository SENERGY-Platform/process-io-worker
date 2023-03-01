/*
 * Copyright (c) 2022 InfAI (CC SES)
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

package handler

import (
	"encoding/json"
	"github.com/SENERGY-Platform/process-io-worker/pkg/configuration"
	"github.com/SENERGY-Platform/process-io-worker/pkg/model"
	"reflect"
	"testing"
	"time"
)

type IoApiMock struct {
	Values map[string]model.VariableWithUnixTimestamp
}

var timestamp = time.Now().Unix()

func (this *IoApiMock) Bulk(userid string, req model.BulkRequest) (outputs model.BulkResponse, err error) {
	if this.Values == nil {
		this.Values = map[string]model.VariableWithUnixTimestamp{}
	}
	for _, value := range req.Set {
		this.Values[value.Key] = model.VariableWithUnixTimestamp{
			Variable:         value,
			UnixTimestampInS: timestamp,
		}
	}
	for _, key := range req.Get {
		variable := this.Values[key]
		variable.Key = key
		outputs = append(outputs, variable)
	}
	return outputs, nil
}

func TestHandler(t *testing.T) {
	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	api := &IoApiMock{}
	handler := NewWithDependencies(config, api)

	outputs, err := handler.Do(model.CamundaExternalTask{
		ProcessInstanceId:   "instance1",
		ProcessDefinitionId: "definition1",
		TenantId:            "user1",
		Variables: map[string]model.CamundaVariable{
			"ignored":                {Value: 13},
			config.WritePrefix + "a": {Value: "a"},
			config.WritePrefix + "instance_" + config.InstanceIdPlaceholder + "_b":            {Value: "b"},
			config.WritePrefix + "definition_" + config.ProcessDefinitionIdPlaceholder + "_c": {Value: "c"},
			config.WritePrefix + "number":  {Value: 42},
			config.WritePrefix + "jsonStr": {Value: `"json-string"`},
			config.WritePrefix + "jsonObj": {Value: `{"foo": true}`},
			config.WritePrefix + "bool":    {Value: true},

			config.ReadPrefix + "with_default1":                                                  {Value: "instance_" + config.InstanceIdPlaceholder + "_with_default1"},
			config.DefaultPrefix + "instance_" + config.InstanceIdPlaceholder + "_with_default1": {Value: "defaultstringvalue1"},

			config.ReadPrefix + "with_default_number":                                                  {Value: "instance_" + config.InstanceIdPlaceholder + "_with_default_number"},
			config.DefaultPrefix + "instance_" + config.InstanceIdPlaceholder + "_with_default_number": {Value: 42},

			config.ReadPrefix + "with_default_json_number":                                                  {Value: "instance_" + config.InstanceIdPlaceholder + "_with_default_json_number"},
			config.DefaultPrefix + "instance_" + config.InstanceIdPlaceholder + "_with_default_json_number": {Value: "42"},

			config.ReadPrefix + "with_default_json_str":                                                  {Value: "instance_" + config.InstanceIdPlaceholder + "_with_default_json_str"},
			config.DefaultPrefix + "instance_" + config.InstanceIdPlaceholder + "_with_default_json_str": {Value: `"defaultstringvalue"`},

			config.WritePrefix + "instance_" + config.InstanceIdPlaceholder + "_with_default2": {Value: "foobar"},

			config.ReadPrefix + "unknown": {Value: "unknown_key"},
		},
	})
	if err != nil {
		t.Error(err)
		return
	}

	expected := map[string]interface{}{
		"unknown":                  nil,
		"with_default1":            "defaultstringvalue1",
		"with_default_number":      42,
		"with_default_json_number": float64(42),
		"with_default_json_str":    "defaultstringvalue",
	}
	if !reflect.DeepEqual(outputs, expected) {
		a, _ := json.Marshal(outputs)
		e, _ := json.Marshal(expected)
		t.Error("\n", string(e), "\n", string(a))
		return
	}

	outputs, err = handler.Do(model.CamundaExternalTask{
		ProcessInstanceId:   "instance1",
		ProcessDefinitionId: "definition1",
		TenantId:            "user1",
		Variables: map[string]model.CamundaVariable{
			"ignored":                       {Value: 13},
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
	})
	if err != nil {
		t.Error(err)
		return
	}

	if !reflect.DeepEqual(outputs, map[string]interface{}{
		"o_a":           "a",
		"o_b":           "b",
		"o_c":           "c",
		"o_number":      42,
		"o_jsonStr":     "json-string",
		"o_jsonObj":     map[string]interface{}{"foo": true},
		"o_bool":        true,
		"with_default1": "defaultstringvalue1",
		"with_default2": "foobar",
	}) {
		t.Error(outputs)
		return
	}

	if !reflect.DeepEqual(api.Values, toVariablesWithTimestamp(map[string]model.Variable{
		"a": {
			Key:                 "a",
			Value:               "a",
			ProcessDefinitionId: "",
			ProcessInstanceId:   "",
		},
		"instance_instance1_b": {
			Key:                 "instance_instance1_b",
			Value:               "b",
			ProcessDefinitionId: "definition1",
			ProcessInstanceId:   "instance1",
		},
		"definition_definition1_c": {
			Key:                 "definition_definition1_c",
			Value:               "c",
			ProcessDefinitionId: "definition1",
			ProcessInstanceId:   "",
		},
		"number": {
			Key:                 "number",
			Value:               42,
			ProcessDefinitionId: "",
			ProcessInstanceId:   "",
		},
		"jsonStr": {
			Key:                 "jsonStr",
			Value:               "json-string",
			ProcessDefinitionId: "",
			ProcessInstanceId:   "",
		},
		"jsonObj": {
			Key:                 "jsonObj",
			Value:               map[string]interface{}{"foo": true},
			ProcessDefinitionId: "",
			ProcessInstanceId:   "",
		},
		"bool": {
			Key:                 "bool",
			Value:               true,
			ProcessDefinitionId: "",
			ProcessInstanceId:   "",
		},
		"instance_instance1_with_default2": {
			Key:                 "instance_instance1_with_default2",
			Value:               "foobar",
			ProcessDefinitionId: "definition1",
			ProcessInstanceId:   "instance1",
		},
	})) {
		t.Error(api.Values)
		return
	}
}

func toVariablesWithTimestamp(m map[string]model.Variable) (result map[string]model.VariableWithUnixTimestamp) {
	result = map[string]model.VariableWithUnixTimestamp{}
	for key, value := range m {
		result[key] = model.VariableWithUnixTimestamp{
			Variable:         value,
			UnixTimestampInS: timestamp,
		}
	}
	return result
}
