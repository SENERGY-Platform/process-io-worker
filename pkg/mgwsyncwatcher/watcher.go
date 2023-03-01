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
	"context"
	"github.com/SENERGY-Platform/process-io-worker/pkg/cache"
	"github.com/SENERGY-Platform/process-io-worker/pkg/configuration"
	"github.com/SENERGY-Platform/process-io-worker/pkg/ioclient"
	paho "github.com/eclipse/paho.mqtt.golang"
	"log"
	"sync"
	"time"
)

func Start(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, client ioclient.IoClient) error {
	c := cache.NewCache(time.Minute)

	options := paho.NewClientOptions().
		SetAutoReconnect(true).
		SetCleanSession(true).
		AddBroker(config.MgwMqttBroker).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			log.Println("mgw-sync-watcher lost connection")
		}).
		SetOnConnectHandler(func(m paho.Client) {
			log.Println("mgw-sync-watcher connected to mqtt broker")
			m.Subscribe(getProcessDefinitionKnownIdsTopic(), 2, func(_ paho.Client, message paho.Message) {
				handleKnownDefinitionsUpdate(message, client, c, config.MgwProcessUser)
			})
			m.Subscribe(getProcessInstanceKnownIdsTopic(), 2, func(_ paho.Client, message paho.Message) {
				handleKnownInstancesUpdate(message, client, c, config.MgwProcessUser)
			})
		})

	mqtt := paho.NewClient(options)
	if token := mqtt.Connect(); token.Wait() && token.Error() != nil {
		log.Println("Error on Mqtt.Connect(): ", token.Error())
		return token.Error()
	}
	go func() {
		<-ctx.Done()
		mqtt.Disconnect(0)
	}()
	return nil
}

const processProcessDefinitionTopic = "process-definition"

func getProcessDefinitionKnownIdsTopic() string {
	return getStateTopic(processProcessDefinitionTopic, "known")
}

const processInstanceTopic = "process-instance"

func getProcessInstanceKnownIdsTopic() string {
	return getStateTopic(processInstanceTopic, "known")
}

func getStateTopic(entity string, substate ...string) (topic string) {
	topic = getBaseTopic() + "/state/" + entity
	for _, sub := range substate {
		topic = topic + "/" + sub
	}
	return
}

func getBaseTopic() string {
	return "processes"
}
