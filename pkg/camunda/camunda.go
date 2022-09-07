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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/process-io-worker/pkg/cache"
	"github.com/SENERGY-Platform/process-io-worker/pkg/camunda/shards"
	"github.com/SENERGY-Platform/process-io-worker/pkg/configuration"
	"github.com/SENERGY-Platform/process-io-worker/pkg/kafka"
	"github.com/SENERGY-Platform/process-io-worker/pkg/model"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

func New(config configuration.Config, handler Handler, incidentProducer Producer, shards Shards) *Camunda {
	return &Camunda{
		config:           config,
		handler:          handler,
		shards:           shards,
		incidentProducer: incidentProducer,
	}
}

func StartDefault(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, handler Handler) error {
	s, err := shards.New(config.ShardsDb, cache.NewCache(time.Minute))
	if err != nil {
		return err
	}
	incidentProducer, err := kafka.NewProducer(ctx, config, config.KafkaIncidentTopic)
	if err != nil {
		return err
	}
	Start(ctx, wg, config, handler, incidentProducer, s)
	return nil
}

func Start(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, handler Handler, incidentProducer Producer, shards Shards) {
	New(config, handler, incidentProducer, shards).Start(ctx, wg)
}

type Shards interface {
	GetShards() (result []string, err error)
	GetShardForUser(userId string) (shardUrl string, err error)
}

type Camunda struct {
	config           configuration.Config
	handler          Handler
	incidentProducer Producer
	shards           Shards
}

type Producer interface {
	Produce(key []byte, value []byte) error
}

type Handler interface {
	Do(task model.CamundaExternalTask) (outputs map[string]interface{}, err error)
}

func (this *Camunda) GetTasks() (tasks []model.CamundaExternalTask, err error) {
	shards, err := this.shards.GetShards()
	if err != nil {
		return tasks, err
	}
	for _, shard := range shards {
		temp, err := this.getShardTasks(shard)
		if err != nil {
			return tasks, err
		}
		tasks = append(tasks, temp...)
	}
	return tasks, nil
}

func (this *Camunda) getShardTasks(shard string) (tasks []model.CamundaExternalTask, err error) {
	fetchRequest := model.CamundaFetchRequest{
		WorkerId: this.GetWorkerId(),
		MaxTasks: this.config.CamundaFetchMaxTasks,
		Topics:   []model.CamundaTopic{{LockDuration: this.config.CamundaLockDurationInMs, Name: this.config.CamundaWorkerTopic}},
	}
	client := http.Client{Timeout: 5 * time.Second}
	b := new(bytes.Buffer)
	err = json.NewEncoder(b).Encode(fetchRequest)
	if err != nil {
		return
	}
	endpoint := shard + "/engine-rest/external-task/fetchAndLock"
	resp, err := client.Post(endpoint, "application/json", b)
	if err != nil {
		return tasks, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		temp, err := io.ReadAll(resp.Body)
		err = errors.New(fmt.Sprintln(endpoint, resp.Status, resp.StatusCode, string(temp), err))
		return tasks, err
	}
	err = json.NewDecoder(resp.Body).Decode(&tasks)
	return
}

func (this *Camunda) Start(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			default:
				wait := this.executeNextTasks()
				if wait {
					duration := time.Duration(this.config.CamundaWorkerWaitDurationInMs) * time.Millisecond
					time.Sleep(duration)
				}
			}
		}
	}()
}

func (this *Camunda) executeNextTasks() (wait bool) {
	tasks, err := this.GetTasks()
	if err != nil {
		log.Println("error on ExecuteNextTasks getTask", err)
		return true
	}
	if len(tasks) == 0 {
		return true
	}
	for _, task := range tasks {
		outputs, err := this.handler.Do(task)
		if err != nil {
			this.Error(task.Id, task.ProcessInstanceId, task.ProcessDefinitionId, err.Error(), task.TenantId)
			continue
		} else {
			err = this.completeTask(task.TenantId, task.Id, outputs)
			if err != nil {
				this.Error(task.Id, task.ProcessInstanceId, task.ProcessDefinitionId, err.Error(), task.TenantId)
				continue
			}
		}
	}
	return false
}

func (this *Camunda) completeTask(userId string, taskId string, outputs map[string]interface{}) (err error) {
	log.Println("Start complete Request")
	shard, err := this.shards.GetShardForUser(userId)
	if err != nil {
		return err
	}

	client := http.Client{Timeout: 5 * time.Second}

	variables := map[string]model.CamundaVariable{}
	for key, value := range outputs {
		variables[key] = model.CamundaVariable{Value: value}
	}

	var completeRequest = model.CamundaCompleteRequest{WorkerId: this.config.CamundaWorkerId, Variables: variables}
	b := new(bytes.Buffer)
	err = json.NewEncoder(b).Encode(completeRequest)
	if err != nil {
		return
	}
	resp, err := client.Post(shard+"/engine-rest/external-task/"+url.PathEscape(taskId)+"/complete", "application/json", b)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	pl, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 300 {
		log.Println("ERROR: unable to complete task:", resp.StatusCode, string(pl))
		return fmt.Errorf("unable to complete task: %v, %v", resp.StatusCode, string(pl))
	} else {
		log.Println("complete camunda task: ", completeRequest, string(pl))
	}
	return nil
}
