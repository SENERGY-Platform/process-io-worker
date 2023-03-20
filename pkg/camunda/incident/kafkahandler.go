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
	"github.com/SENERGY-Platform/process-io-worker/pkg/kafka"
	"github.com/SENERGY-Platform/process-io-worker/pkg/model"
	"log"
	"runtime/debug"
)

func NewKafkaIncidentHandler(ctx context.Context, config configuration.Config) (*KafkaIncidentHandler, error) {
	producer, err := kafka.NewProducer(ctx, config, config.KafkaIncidentTopic)
	if err != nil {
		return nil, err
	}
	return &KafkaIncidentHandler{
		producer: producer,
	}, nil
}

type KafkaIncidentHandler struct {
	producer *kafka.Producer
}

func (this *KafkaIncidentHandler) Handle(incident model.Incident) error {
	b, err := json.Marshal(model.KafkaIncidentsCommand{
		Command:             "POST",
		MsgVersion:          3,
		Incident:            &incident,
		ProcessDefinitionId: incident.ProcessDefinitionId,
		ProcessInstanceId:   incident.ProcessInstanceId,
	})
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return err
	}
	err = this.producer.Produce([]byte(incident.ProcessDefinitionId), b)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return err
	}
	return nil
}
