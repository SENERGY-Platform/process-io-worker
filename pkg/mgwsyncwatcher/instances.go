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

package mgwsyncwatcher

import (
	"encoding/json"
	"github.com/SENERGY-Platform/process-io-worker/pkg/cache"
	"github.com/SENERGY-Platform/process-io-worker/pkg/ioclient"
	paho "github.com/eclipse/paho.mqtt.golang"
	"log"
)

// handleKnownInstancesUpdate watches mgw process sync messages to delete process-io-variables of removed process-instances
func handleKnownInstancesUpdate(message paho.Message, client ioclient.IoClient, c *cache.CacheImpl, userId string) {
	ids := []string{}
	err := json.Unmarshal(message.Payload(), &ids)
	if err != nil {
		log.Println("ERROR: unable to unmarshal message:", message.Topic(), string(message.Payload()), err)
		return
	}

	knownIds := map[string]bool{}
	for _, id := range ids {
		knownIds[id] = true
	}

	list, err := getList(client, c, userId)
	if err != nil {
		return
	}
	variableInstanceIdsToDelete := map[string]bool{}
	for _, v := range list {
		if v.ProcessInstanceId != "" && !knownIds[v.ProcessInstanceId] {
			variableInstanceIdsToDelete[v.ProcessInstanceId] = true
		}
	}
	for instanceId, _ := range variableInstanceIdsToDelete {
		err = client.DeleteProcessInstance(userId, instanceId)
		if err != nil {
			log.Println("ERROR: unable to delete variables by process-instance-id", instanceId, err)
		}
	}
}
