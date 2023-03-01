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

package camunda

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"time"
)

func (this *Camunda) GetProcessName(userId string, processDefinitionId string) (name string, err error) {
	shard, err := this.shards.GetShardForUser(userId)
	if err != nil {
		return name, err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	request, err := http.NewRequest("GET", shard+"/engine-rest/process-definition/"+url.PathEscape(processDefinitionId), nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		temp, _ := io.ReadAll(resp.Body)
		log.Println("ERROR:", resp.Status, string(temp))
		debug.PrintStack()
		return "", errors.New("unexpected response")
	}
	result := NameWrapper{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	return result.Name, err
}

type NameWrapper struct {
	Name string `json:"name"`
}
