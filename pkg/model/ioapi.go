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

package model

type BulkSetElement = Variable

type BulkResponse = []VariableWithUnixTimestamp

type Variable struct {
	Key                 string      `json:"key"`
	Value               interface{} `json:"value"`
	ProcessDefinitionId string      `json:"process_definition_id,omitempty"`
	ProcessInstanceId   string      `json:"process_instance_id,omitempty"`
}

type VariableWithUnixTimestamp struct {
	Variable
	UnixTimestampInS int64 `json:"unix_timestamp_in_s"`
}
