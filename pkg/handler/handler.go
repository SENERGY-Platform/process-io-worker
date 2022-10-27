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
	"fmt"
	"github.com/SENERGY-Platform/process-io-worker/pkg/auth"
	"github.com/SENERGY-Platform/process-io-worker/pkg/configuration"
	"github.com/SENERGY-Platform/process-io-worker/pkg/ioapi"
	"github.com/SENERGY-Platform/process-io-worker/pkg/model"
	"strings"
)

func New(config configuration.Config) *Handler {
	return NewWithDependencies(config, auth.New(config), ioapi.New(config))
}

func NewWithDependencies(config configuration.Config, auth Auth, ioApi IoApi) *Handler {
	return &Handler{
		config: config,
		api:    ioApi,
		auth:   auth,
	}
}

type Handler struct {
	config configuration.Config
	api    IoApi
	auth   Auth
}

type Auth interface {
	ExchangeUserToken(userid string) (token auth.Token, err error)
}

type IoApi interface {
	Bulk(token auth.Token, set []model.BulkSetElement, get []string) (outputs model.BulkResponse, err error)
}

func (this *Handler) Do(task model.CamundaExternalTask) (outputs map[string]interface{}, err error) {
	outputs = map[string]interface{}{}

	token, err := this.auth.ExchangeUserToken(task.TenantId)
	if err != nil {
		return nil, err
	}

	get := []string{}
	getKeyToOutput := map[string]string{}
	keyToDefault := map[string]interface{}{}

	set := []model.BulkSetElement{}

	for varName, variable := range task.Variables {
		if strings.HasPrefix(varName, this.config.ReadPrefix) {
			outputName := strings.TrimPrefix(varName, this.config.ReadPrefix)
			rawKey, ok := variable.Value.(string)
			if !ok {
				return outputs, fmt.Errorf("unable to interpret value of %v as string", varName)
			}
			key := this.resolveKeyPlaceholders(task, rawKey)
			get = append(get, key)
			getKeyToOutput[key] = outputName

			if defaultValueInput, withDefault := task.Variables[this.config.DefaultPrefix+rawKey]; withDefault {
				keyToDefault[key] = resolveValue(defaultValueInput.Value)
			}
		}
		if strings.HasPrefix(varName, this.config.WritePrefix) {
			key := strings.TrimPrefix(varName, this.config.WritePrefix)
			key = this.resolveKeyPlaceholders(task, key)
			value := resolveValue(variable.Value)
			setElement := model.BulkSetElement{
				Key:                 key,
				Value:               value,
				ProcessDefinitionId: "",
				ProcessInstanceId:   "",
			}
			setElement.ProcessDefinitionId, setElement.ProcessInstanceId = this.getUsedProcessIds(task, varName)
			set = append(set, setElement)
		}
	}
	bulkResult, err := this.api.Bulk(token, set, get)
	if err != nil {
		return outputs, err
	}
	for _, variable := range bulkResult {
		if variable.Value == nil {
			outputs[getKeyToOutput[variable.Key]] = keyToDefault[variable.Key]
		} else {
			outputs[getKeyToOutput[variable.Key]] = variable.Value
		}
	}
	return outputs, err
}

func resolveValue(value interface{}) (result interface{}) {
	result = value
	valueAsString, ok := result.(string)
	if ok {
		var valueAsJson interface{}
		err := json.Unmarshal([]byte(valueAsString), &valueAsJson)
		if err != nil {
			result = valueAsString
		} else {
			result = valueAsJson
		}
	}
	return result
}

func (this *Handler) resolveKeyPlaceholders(task model.CamundaExternalTask, key string) string {
	result := strings.ReplaceAll(key, this.config.InstanceIdPlaceholder, task.ProcessInstanceId)
	result = strings.ReplaceAll(result, this.config.ProcessDefinitionIdPlaceholder, task.ProcessDefinitionId)
	return result
}

func (this *Handler) getUsedProcessIds(task model.CamundaExternalTask, variableName string) (string, string) {
	if strings.Contains(variableName, this.config.InstanceIdPlaceholder) {
		return task.ProcessDefinitionId, task.ProcessInstanceId
	}
	if strings.Contains(variableName, this.config.ProcessDefinitionIdPlaceholder) {
		return task.ProcessDefinitionId, ""
	}
	return "", ""
}
