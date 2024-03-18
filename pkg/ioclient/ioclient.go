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

package ioclient

import (
	"context"
	"errors"
	"github.com/SENERGY-Platform/process-io-api/pkg/api/client"
	"github.com/SENERGY-Platform/process-io-api/pkg/api/client/auth"
	ctrlconf "github.com/SENERGY-Platform/process-io-api/pkg/configuration"
	"github.com/SENERGY-Platform/process-io-api/pkg/controller"
	"github.com/SENERGY-Platform/process-io-api/pkg/database"
	"github.com/SENERGY-Platform/process-io-api/pkg/model"
	"github.com/SENERGY-Platform/process-io-worker/pkg/configuration"
	"github.com/SENERGY-Platform/service-commons/pkg/cache"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/localcache"
	"sync"
	"time"
)

type IoClient interface {
	Bulk(userid string, req model.BulkRequest) (outputs model.BulkResponse, err error)
	List(userid string, query model.VariablesQueryOptions) ([]model.VariableWithUnixTimestamp, error)
	Count(userId string, query model.VariablesQueryOptions) (model.Count, error)
	DeleteProcessDefinition(userid string, definitionId string) error
	DeleteProcessInstance(userid string, instanceId string) error
}

func New(ctx context.Context, wg *sync.WaitGroup, config configuration.Config) (IoClient, error) {
	switch config.IoDataSource {
	case configuration.ApiClient:
		c, err := cache.New(cache.Config{L1Provider: localcache.NewProvider(time.Duration(config.TokenCacheDefaultExpirationInSeconds)*time.Second, time.Second)})
		if err != nil {
			return nil, err
		}
		a, err := auth.New(config.AuthEndpoint, config.AuthClientId, config.AuthClientSecret, c)
		if err != nil {
			return nil, err
		}
		return client.NewWithAuth(config.IoApiUrl, a, config.Debug), nil
	case configuration.MongoDb:
		fallthrough
	case configuration.PostgresDb:
		ctrlConfig := ctrlconf.Config{
			Debug:                    config.Debug,
			DisableHttpLogger:        true,
			EnableSwaggerUi:          false,
			DatabaseSelection:        config.IoDataSource,
			MongoUrl:                 config.MongoUrl,
			MongoTable:               config.MongoTable,
			MongoVariablesCollection: config.MongoVariablesCollection,
			PostgresConnString:       config.PostgresConnString,
		}
		db, err := database.New(ctx, wg, ctrlConfig)
		if err != nil {
			return nil, err
		}
		return controller.New(ctrlConfig, db), nil
	default:
		return nil, errors.New("unknown io data source: " + config.IoDataSource)
	}
}
