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
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"
)

func (this *Camunda) StopProcessInstance(userId string, processInstanceId string) (err error) {
	shard, err := this.shards.GetShardForUser(userId)
	if err != nil {
		return err
	}
	client := http.Client{Timeout: 5 * time.Second}
	request, err := http.NewRequest("DELETE", shard+"/engine-rest/process-instance/"+url.PathEscape(processInstanceId)+"?skipIoMappings=true", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode == 200 || resp.StatusCode == 204 {
		return nil
	}
	msg, _ := io.ReadAll(resp.Body)
	err = errors.New("error on delete in engine for " + shard + "/engine-rest/process-instance/" + url.PathEscape(processInstanceId) + ": " + resp.Status + " " + string(msg))
	return err
}
