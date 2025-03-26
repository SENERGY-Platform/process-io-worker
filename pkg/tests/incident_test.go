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
	"errors"
	"github.com/SENERGY-Platform/process-io-worker/pkg/camunda"
	"github.com/SENERGY-Platform/process-io-worker/pkg/configuration"
	"github.com/SENERGY-Platform/process-io-worker/pkg/model"
	"github.com/SENERGY-Platform/process-io-worker/pkg/tests/docker"
	"github.com/SENERGY-Platform/process-io-worker/pkg/tests/mocks"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestWorkerKafkaIncident(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	config.IncidentHandler = configuration.KafkaIncidentHandler

	_, zkIp, err := docker.Zookeeper(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}
	zkUrl := zkIp + ":2181"

	config.KafkaUrl, err = docker.Kafka(ctx, wg, zkUrl)
	if err != nil {
		t.Error(err)
		return
	}

	log.Println("start incidentEnv()")

	camundamock, err := incidentEnv(ctx, wg, config)
	if err != nil {
		t.Error(err)
		t.Logf("%#v\n", config)
		return
	}

	log.Println("start test kafka.NewReader()")

	kafkaIncidents := []model.KafkaIncidentsCommand{}
	r := kafka.NewReader(kafka.ReaderConfig{
		CommitInterval: 0, //synchronous commits
		GroupID:        "test",
		Brokers:        []string{config.KafkaUrl},
		Topic:          config.KafkaIncidentTopic,
		MaxWait:        1 * time.Second,
		Logger:         log.New(os.Stdout, "[KAFKA] ", 0),
		ErrorLogger:    log.New(os.Stderr, "[KAFKA-ERROR] ", 0),
	})
	defer r.Close()

	func() {
		timeout, _ := context.WithTimeout(ctx, 10*time.Second)
		for {
			select {
			case <-timeout.Done():
				log.Println("test kafka.NewReader() done")
				return
			default:
				m, err := r.FetchMessage(timeout)
				log.Println("test kafka.NewReader() fetch")
				if err == io.EOF || err == context.Canceled || err == context.DeadlineExceeded {
					log.Println("test kafka.NewReader() cancel")
					return
				}
				if err != nil {
					log.Println("test kafka.NewReader() err", err)
					t.Error(err)
					return
				}
				incident := model.KafkaIncidentsCommand{}
				err = json.Unmarshal(m.Value, &incident)
				if err != nil {
					t.Error(err)
					return
				}
				if incident.Incident != nil {
					incident.Incident.Id = ""
					incident.Incident.Time = time.Time{}
				}
				kafkaIncidents = append(kafkaIncidents, incident)
				err = r.CommitMessages(ctx, m)
				if err != nil {
					log.Println("test kafka.NewReader() commit", err)
					t.Error(err)
					return
				}
			}
		}
	}()

	expectedKafkaIncidents := []model.KafkaIncidentsCommand{
		{Command: "POST", MsgVersion: 3, Incident: &model.Incident{Id: "", MsgVersion: 0, ExternalTaskId: "task1", ProcessInstanceId: "instance1", ProcessDefinitionId: "definition1", WorkerId: "process_io", ErrorMessage: "mock error", Time: time.Time{}, TenantId: "user1", DeploymentName: ""}, ProcessInstanceId: "instance1", ProcessDefinitionId: "definition1"},
		{Command: "POST", MsgVersion: 3, Incident: &model.Incident{Id: "", MsgVersion: 0, ExternalTaskId: "task2", ProcessInstanceId: "instance2", ProcessDefinitionId: "definition2", WorkerId: "process_io", ErrorMessage: "mock error", Time: time.Time{}, TenantId: "user1", DeploymentName: ""}, ProcessInstanceId: "instance2", ProcessDefinitionId: "definition2"},
	}

	if !reflect.DeepEqual(jsonNormalize(kafkaIncidents), jsonNormalize(expectedKafkaIncidents)) {
		t.Errorf("\n%#v\n%#v", jsonNormalize(expectedKafkaIncidents), jsonNormalize(kafkaIncidents))
	}

	if len(camundamock.GetUnexpectedRequests()) > 0 {
		t.Error(camundamock.GetUnexpectedRequests())
		return
	}
	if len(camundamock.GetCompleteRequests()) > 0 {
		t.Error(camundamock.GetCompleteRequests())
		return
	}

	expectedCamundaStopRequests := map[string][]interface{}{}
	actual := camundamock.GetStopRequests()
	if !reflect.DeepEqual(actual, expectedCamundaStopRequests) {
		t.Errorf("\n%#v\n%#v", expectedCamundaStopRequests, actual)
	}
}

func TestWorkerCamundaIncident(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	config.IncidentHandler = configuration.CamundaIncidentHandler

	camundamock, err := incidentEnv(ctx, wg, config)
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
	if len(camundamock.GetCompleteRequests()) > 0 {
		t.Error(camundamock.GetCompleteRequests())
		return
	}

	expectedCamundaStopRequests := map[string][]interface{}{
		"/engine-rest/process-instance/instance1?skipIoMappings=true": {nil},
		"/engine-rest/process-instance/instance2?skipIoMappings=true": {nil},
	}
	actual := camundamock.GetStopRequests()
	if !reflect.DeepEqual(actual, expectedCamundaStopRequests) {
		t.Errorf("\n%#v\n%#v", expectedCamundaStopRequests, actual)
	}
}

func TestWorkerMgwIncident(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	config.IncidentHandler = configuration.MgwIncidentHandler

	mgwMqttPort, _, err := docker.Mqtt(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}
	config.MgwMqttBroker = "tcp://localhost:" + mgwMqttPort

	camundamock, err := incidentEnv(ctx, wg, config)
	if err != nil {
		t.Error(err)
		t.Logf("%#v\n", config)
		return
	}

	mgwmqttclient := paho.NewClient(paho.NewClientOptions().
		SetAutoReconnect(true).
		SetCleanSession(false).
		SetClientID("test-client-mgw").
		AddBroker(config.MgwMqttBroker))
	if token := mgwmqttclient.Connect(); token.Wait() && token.Error() != nil {
		log.Println("Error on Mqtt.Connect(): ", token.Error())
		t.Error(err)
		return
	}
	defer mgwmqttclient.Disconnect(0)

	mqttIncidentMessages := []model.Incident{}
	mgwmqttclient.Subscribe("processes/state/incident", 2, func(client paho.Client, message paho.Message) {
		incident := model.Incident{}
		err = json.Unmarshal(message.Payload(), &incident)
		if err != nil {
			t.Error(err)
			return
		}
		incident.Id = ""
		incident.Time = time.Time{}
		mqttIncidentMessages = append(mqttIncidentMessages, incident)
	})

	time.Sleep(2 * time.Second)

	if len(camundamock.GetUnexpectedRequests()) > 0 {
		t.Error(camundamock.GetUnexpectedRequests())
		return
	}
	if len(camundamock.GetCompleteRequests()) > 0 {
		t.Error(camundamock.GetCompleteRequests())
		return
	}

	expectedCamundaStopRequests := map[string][]interface{}{
		"/engine-rest/process-instance/instance1?skipIoMappings=true": {nil},
		"/engine-rest/process-instance/instance2?skipIoMappings=true": {nil},
	}
	actual := camundamock.GetStopRequests()
	if !reflect.DeepEqual(actual, expectedCamundaStopRequests) {
		t.Errorf("\n%#v\n%#v", expectedCamundaStopRequests, actual)
	}

	expectedMqttMessages := []model.Incident{
		{Id: "", MsgVersion: 0, ExternalTaskId: "task1", ProcessInstanceId: "instance1", ProcessDefinitionId: "definition1", WorkerId: "process_io", ErrorMessage: "mock error", Time: time.Time{}, TenantId: "user1", DeploymentName: "name_of_definition1"},
		{Id: "", MsgVersion: 0, ExternalTaskId: "task2", ProcessInstanceId: "instance2", ProcessDefinitionId: "definition2", WorkerId: "process_io", ErrorMessage: "mock error", Time: time.Time{}, TenantId: "user1", DeploymentName: "name_of_definition2"},
	}
	if !reflect.DeepEqual(mqttIncidentMessages, expectedMqttMessages) {
		t.Errorf("\n%#v\n%#v", expectedMqttMessages, mqttIncidentMessages)
	}
}

func TestWorkerHttpIncident(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	config.IncidentHandler = configuration.HttpIncidentHandler

	incidentMessages := []model.Incident{}
	incidentsApiMockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var incident model.Incident
		err = json.NewDecoder(r.Body).Decode(&incident)
		if err != nil {
			t.Error(err)
			return
		}
		incident.Id = ""
		incident.Time = time.Time{}
		incidentMessages = append(incidentMessages, incident)
	}))
	defer incidentsApiMockServer.Close()
	config.IncidentApiUrl = incidentsApiMockServer.URL

	camundamock, err := incidentEnv(ctx, wg, config)
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
	if len(camundamock.GetCompleteRequests()) > 0 {
		t.Error(camundamock.GetCompleteRequests())
		return
	}

	expectedMqttMessages := []model.Incident{
		{Id: "", MsgVersion: 0, ExternalTaskId: "task1", ProcessInstanceId: "instance1", ProcessDefinitionId: "definition1", WorkerId: "process_io", ErrorMessage: "mock error", Time: time.Time{}, TenantId: "user1"},
		{Id: "", MsgVersion: 0, ExternalTaskId: "task2", ProcessInstanceId: "instance2", ProcessDefinitionId: "definition2", WorkerId: "process_io", ErrorMessage: "mock error", Time: time.Time{}, TenantId: "user1"},
	}
	if !reflect.DeepEqual(incidentMessages, expectedMqttMessages) {
		t.Errorf("\n%#v\n%#v", expectedMqttMessages, incidentMessages)
	}
}

func incidentEnv(ctx context.Context, wg *sync.WaitGroup, config configuration.Config) (camundamock *mocks.CamundaMock, err error) {
	config.CamundaWorkerWaitDurationInMs = 100

	camundamock = mocks.NewCamundaMock(ctx, []interface{}{
		[]model.CamundaExternalTask{
			{
				Id:                  "task1",
				ProcessInstanceId:   "instance1",
				ProcessDefinitionId: "definition1",
				TenantId:            "user1",
				Variables:           map[string]model.CamundaVariable{},
			},
		},
		[]model.CamundaExternalTask{
			{
				Id:                  "task2",
				ProcessInstanceId:   "instance2",
				ProcessDefinitionId: "definition2",
				TenantId:            "user1",
				Variables:           map[string]model.CamundaVariable{},
			},
		},
	})

	config.CamundaUrl = camundamock.Server.URL

	time.Sleep(200 * time.Millisecond)

	return camundamock, camunda.StartDefault(ctx, wg, config, ErrorTrowingMockHandler{})
}

type ErrorTrowingMockHandler struct{}

func (this ErrorTrowingMockHandler) Do(task model.CamundaExternalTask) (outputs map[string]interface{}, err error) {
	return nil, errors.New("mock error")
}
