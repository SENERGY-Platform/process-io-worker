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
	model2 "github.com/SENERGY-Platform/process-io-api/pkg/model"
	"github.com/SENERGY-Platform/process-io-worker/pkg/configuration"
	"github.com/SENERGY-Platform/process-io-worker/pkg/ioclient"
	"github.com/SENERGY-Platform/process-io-worker/pkg/mgwsyncwatcher"
	"github.com/SENERGY-Platform/process-io-worker/pkg/model"
	"github.com/SENERGY-Platform/process-io-worker/pkg/tests/docker"
	paho "github.com/eclipse/paho.mqtt.golang"
	"log"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestMgwProcessSyncWatcher(t *testing.T) {
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

	source := configuration.MongoDb

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

	mgwMqttPort, _, err := docker.Mqtt(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}
	config.MgwMqttBroker = "tcp://localhost:" + mgwMqttPort

	c, err := ioclient.New(ctx, wg, config)
	if err != nil {
		t.Error(err)
		return
	}

	err = mgwsyncwatcher.Start(ctx, wg, config, c)
	if err != nil {
		t.Error(err)
		return
	}

	userid := config.MgwProcessUser

	t.Run("create variables", func(t *testing.T) {
		list := []model.Variable{
			{Key: "a", Value: "a", ProcessDefinitionId: "", ProcessInstanceId: ""},
			{Key: "b", Value: "b", ProcessDefinitionId: "", ProcessInstanceId: ""},
			{Key: "c", Value: "c", ProcessDefinitionId: "", ProcessInstanceId: ""},
			{Key: "d1i1a", Value: "a", ProcessDefinitionId: "d1", ProcessInstanceId: "i1"},
			{Key: "d1i1b", Value: "b", ProcessDefinitionId: "d1", ProcessInstanceId: "i1"},
			{Key: "d1i2c", Value: "c", ProcessDefinitionId: "d1", ProcessInstanceId: "i2"},
			{Key: "d1i2d", Value: "d", ProcessDefinitionId: "d1", ProcessInstanceId: "i2"},
			{Key: "d2i1a", Value: "a", ProcessDefinitionId: "d2", ProcessInstanceId: ""},
			{Key: "d2i1b", Value: "b", ProcessDefinitionId: "d2", ProcessInstanceId: ""},
			{Key: "d2i2c", Value: "c", ProcessDefinitionId: "d2", ProcessInstanceId: ""},
		}
		for _, e := range list {
			err = apictrl.Set(userid, e)
			if err != nil {
				t.Error(err)
				return
			}
		}
	})

	time.Sleep(2 * time.Second)

	t.Run("list created variables", func(t *testing.T) {
		list, err := apictrl.List(userid, model2.VariablesQueryOptions{
			Limit:  100,
			Offset: 0,
		})
		if err != nil {
			t.Error(err)
			return
		}
		actual := []model.Variable{}
		for _, e := range list {
			actual = append(actual, e.Variable)
		}
		expected := []model.Variable{
			{Key: "a", Value: "a", ProcessDefinitionId: "", ProcessInstanceId: ""},
			{Key: "b", Value: "b", ProcessDefinitionId: "", ProcessInstanceId: ""},
			{Key: "c", Value: "c", ProcessDefinitionId: "", ProcessInstanceId: ""},
			{Key: "d1i1a", Value: "a", ProcessDefinitionId: "d1", ProcessInstanceId: "i1"},
			{Key: "d1i1b", Value: "b", ProcessDefinitionId: "d1", ProcessInstanceId: "i1"},
			{Key: "d1i2c", Value: "c", ProcessDefinitionId: "d1", ProcessInstanceId: "i2"},
			{Key: "d1i2d", Value: "d", ProcessDefinitionId: "d1", ProcessInstanceId: "i2"},
			{Key: "d2i1a", Value: "a", ProcessDefinitionId: "d2", ProcessInstanceId: ""},
			{Key: "d2i1b", Value: "b", ProcessDefinitionId: "d2", ProcessInstanceId: ""},
			{Key: "d2i2c", Value: "c", ProcessDefinitionId: "d2", ProcessInstanceId: ""},
		}
		sort.Slice(expected, func(i, j int) bool {
			return expected[i].Key < expected[j].Key
		})
		sort.Slice(actual, func(i, j int) bool {
			return actual[i].Key < actual[j].Key
		})
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("\n%#v\n%#v", expected, actual)
		}
	})

	options := paho.NewClientOptions().
		SetAutoReconnect(true).
		SetCleanSession(true).
		AddBroker(config.MgwMqttBroker).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			log.Println("mgw-sync-watcher lost connection")
		})

	mqtt := paho.NewClient(options)
	if token := mqtt.Connect(); token.Wait() && token.Error() != nil {
		t.Error(token.Error())
		return
	}

	t.Run("delete process definition", func(t *testing.T) {
		token := mqtt.Publish("processes/state/process-definition/known", 2, false, `["d1"]`)
		if token.Wait() && token.Error() != nil {
			t.Error(token.Error())
			return
		}
	})

	t.Run("delete process instance", func(t *testing.T) {
		token := mqtt.Publish("processes/state/process-instance/known", 2, false, `["i1"]`)
		if token.Wait() && token.Error() != nil {
			t.Error(token.Error())
			return
		}
	})

	time.Sleep(2 * time.Second)

	t.Run("list variables after delete", func(t *testing.T) {
		list, err := apictrl.List(userid, model2.VariablesQueryOptions{
			Limit:  100,
			Offset: 0,
		})
		if err != nil {
			t.Error(err)
			return
		}
		actual := []model.Variable{}
		for _, e := range list {
			actual = append(actual, e.Variable)
		}
		expected := []model.Variable{
			{Key: "a", Value: "a", ProcessDefinitionId: "", ProcessInstanceId: ""},
			{Key: "b", Value: "b", ProcessDefinitionId: "", ProcessInstanceId: ""},
			{Key: "c", Value: "c", ProcessDefinitionId: "", ProcessInstanceId: ""},
			{Key: "d1i1a", Value: "a", ProcessDefinitionId: "d1", ProcessInstanceId: "i1"},
			{Key: "d1i1b", Value: "b", ProcessDefinitionId: "d1", ProcessInstanceId: "i1"},
		}
		sort.Slice(expected, func(i, j int) bool {
			return expected[i].Key < expected[j].Key
		})
		sort.Slice(actual, func(i, j int) bool {
			return actual[i].Key < actual[j].Key
		})
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("\n%#v\n%#v", expected, actual)
		}
	})
}
