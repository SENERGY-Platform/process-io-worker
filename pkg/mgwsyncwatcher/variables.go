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
	"github.com/SENERGY-Platform/process-io-api/pkg/model"
	"github.com/SENERGY-Platform/process-io-worker/pkg/cache"
	"github.com/SENERGY-Platform/process-io-worker/pkg/ioclient"
	"log"
)

func getList(client ioclient.IoClient, c *cache.CacheImpl, userId string) ([]model.VariableWithUnixTimestamp, error) {
	count := model.Count{}
	err := c.Use("count", func() (interface{}, error) {
		return client.Count(userId, model.VariablesQueryOptions{})
	}, &count)
	if err != nil {
		log.Println("ERROR: unable to count io-variables", err)
		return nil, err
	}
	list := []model.VariableWithUnixTimestamp{}
	err = c.Use("list", func() (interface{}, error) {
		return client.List(userId, model.VariablesQueryOptions{Limit: int(count.Count), Offset: 0})
	}, &list)
	if err != nil {
		log.Println("ERROR: unable to list io-variables", err)
		return nil, err
	}
	return list, nil
}
