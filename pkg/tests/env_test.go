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
	"errors"
	"github.com/SENERGY-Platform/process-io-api/pkg"
	"github.com/SENERGY-Platform/process-io-api/pkg/configuration"
	"github.com/SENERGY-Platform/process-io-api/pkg/controller"
	"github.com/SENERGY-Platform/process-io-worker/pkg/tests/docker"
	"net"
	"strconv"
	"sync"
	"time"
)

func StartApiEnv(ctx context.Context, wg *sync.WaitGroup, dbSelection string) (config configuration.Config, ctrl *controller.Controller, err error) {
	freePort, err := GetFreePort()
	if err != nil {
		return config, ctrl, err
	}
	config = configuration.Config{
		ServerPort:               strconv.Itoa(freePort),
		Debug:                    true,
		DisableHttpLogger:        true,
		EnableSwaggerUi:          false,
		DatabaseSelection:        dbSelection,
		MongoUrl:                 "",
		MongoTable:               "process_io",
		MongoVariablesCollection: "variables",
		PostgresConnString:       "",
	}
	switch dbSelection {
	case "mongodb":
		port, _, err := docker.Mongo(ctx, wg)
		if err != nil {
			return config, nil, err
		}
		config.MongoUrl = "mongodb://localhost:" + port
	case "postgres":
		config.PostgresConnString, err = docker.Postgres(ctx, wg, "test")
		if err != nil {
			return config, nil, err
		}
	default:
		return config, nil, errors.New("unknown database: " + dbSelection)
	}

	ctrl, err = pkg.Start(ctx, wg, config)

	time.Sleep(200 * time.Millisecond)

	return config, ctrl, err
}

func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}
