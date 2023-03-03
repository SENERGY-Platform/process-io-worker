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
	"github.com/SENERGY-Platform/process-io-worker/pkg/configuration"
	"testing"
)

func TestWorkerKafkaIncident(t *testing.T) {
	t.Skip("not implemented")
	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(config, err)
}

func TestWorkerCamundaIncident(t *testing.T) {
	t.Skip("not implemented")
	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(config, err)
}

func TestWorkerMgwIncident(t *testing.T) {
	t.Skip("not implemented")
	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}

	t.Log(config, err)
}
