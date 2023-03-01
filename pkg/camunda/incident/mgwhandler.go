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

package incident

import (
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/process-io-worker/pkg/configuration"
	"github.com/SENERGY-Platform/process-io-worker/pkg/model"
	paho "github.com/eclipse/paho.mqtt.golang"
	"log"
)

func NewMgwIncidentHandler(ctx context.Context, config configuration.Config, c Camunda) (*MgwIncidentHandler, error) {
	options := paho.NewClientOptions().
		SetAutoReconnect(true).
		SetCleanSession(true).
		AddBroker(config.MgwMqttBroker).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			log.Println("incident producer to mqtt broker lost connection")
		}).
		SetOnConnectHandler(func(m paho.Client) {
			log.Println("incident producer connected to mqtt broker")
		})

	client := paho.NewClient(options)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Println("Error on Mqtt.Connect(): ", token.Error())
		return nil, token.Error()
	}
	go func() {
		<-ctx.Done()
		client.Disconnect(0)
	}()
	return &MgwIncidentHandler{camunda: c, mqtt: client}, nil
}

type MgwIncidentHandler struct {
	camunda Camunda
	mqtt    paho.Client
}

func (this *MgwIncidentHandler) Handle(incident model.Incident) error {
	deploymentName, err := this.stopProcessInCamunda(incident)
	if err != nil {
		return err
	}
	return this.sendIncidentToMgw(incident, deploymentName)
}

func (this *MgwIncidentHandler) stopProcessInCamunda(incident model.Incident) (deploymentName string, err error) {
	err = this.camunda.StopProcessInstance(incident.TenantId, incident.ProcessInstanceId)
	if err != nil {
		return deploymentName, err
	}
	name, err := this.camunda.GetProcessName(incident.TenantId, incident.ProcessDefinitionId)
	if err != nil {
		log.Println("WARNING: unable to get process name", err)
		return incident.ProcessDefinitionId, nil
	} else {
		return name, nil
	}
}

func (this *MgwIncidentHandler) sendIncidentToMgw(incident model.Incident, deploymentName string) error {
	incident.DeploymentName = deploymentName
	return this.send(this.getStateTopic(incidentTopic), incident)
}

func (this *MgwIncidentHandler) send(topic string, message interface{}) error {
	msg, err := json.Marshal(message)
	if err != nil {
		return err
	}
	token := this.mqtt.Publish(topic, 2, false, msg)
	token.Wait()
	return token.Error()
}

const incidentTopic = "incident"

func (this *MgwIncidentHandler) getStateTopic(entity string, substate ...string) (topic string) {
	topic = this.getBaseTopic() + "/state/" + entity
	for _, sub := range substate {
		topic = topic + "/" + sub
	}
	return
}

func (this *MgwIncidentHandler) getBaseTopic() string {
	return "processes"
}
