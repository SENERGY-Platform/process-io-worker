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

package camunda

import (
	"encoding/json"
	"github.com/SENERGY-Platform/process-io-worker/pkg/model"
	"github.com/google/uuid"
	"log"
	"runtime/debug"
	"time"
)

func (this *Camunda) Error(externalTaskId string, processInstanceId string, processDefinitionId string, msg string, tenantId string) {
	b, err := json.Marshal(model.KafkaIncidentsCommand{
		Command:    "POST",
		MsgVersion: 3,
		Incident: &model.Incident{
			Id:                  uuid.NewString(),
			ExternalTaskId:      externalTaskId,
			ProcessInstanceId:   processInstanceId,
			ProcessDefinitionId: processDefinitionId,
			WorkerId:            this.GetWorkerId(),
			ErrorMessage:        msg,
			Time:                time.Now(),
			TenantId:            tenantId,
		},
	})
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return
	}
	err = this.incidentProducer.Produce([]byte(processDefinitionId), b)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return
	}
}

func (this *Camunda) GetWorkerId() string {
	return this.config.CamundaWorkerId
}
