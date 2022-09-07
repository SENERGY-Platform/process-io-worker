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

package configuration

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type Config struct {
	ShardsDb                      string `json:"shards_db" config:"secret"`
	CamundaWorkerId               string `json:"camunda_worker_id"`
	CamundaWorkerTopic            string `json:"camunda_worker_topic"`
	CamundaLockDurationInMs       int64  `json:"camunda_lock_duration_in_ms"`
	CamundaWorkerWaitDurationInMs int64  `json:"camunda_worker_wait_duration_in_ms"`
	CamundaFetchMaxTasks          int64  `json:"camunda_fetch_max_tasks"`

	AuthEndpoint                         string `json:"auth_endpoint"`
	AuthClientId                         string `json:"auth_client_id" config:"secret"`
	AuthClientSecret                     string `json:"auth_client_secret" config:"secret"`
	TokenCacheDefaultExpirationInSeconds int    `json:"token_cache_default_expiration_in_seconds"`

	KafkaUrl           string `json:"kafka_url"`
	ConsumerGroup      string `json:"consumer_group"`
	KafkaIncidentTopic string `json:"kafka_incident_topic"`

	IoApiUrl string `json:"io_api_url"`

	Debug bool `json:"debug"`

	ReadPrefix                     string `json:"read_prefix"`
	WritePrefix                    string `json:"write_prefix"`
	InstanceIdPlaceholder          string `json:"instance_id_placeholder"`
	ProcessDefinitionIdPlaceholder string `json:"process_definition_id_placeholder"`
}

func LoadLibConfig(location string) (config Config, err error) {
	return Load[Config](location)
}

// loads config from json in location and used environment variables (e.g KafkaUrl --> KAFKA_URL)
func Load[T any](location string) (config T, err error) {
	file, err := os.Open(location)
	if err != nil {
		return config, err
	}
	err = json.NewDecoder(file).Decode(&config)
	if err != nil {
		return config, err
	}
	handleEnvironmentVars(&config)
	return config, nil
}

var camel = regexp.MustCompile("(^[^A-Z]*|[A-Z]*)([A-Z][^A-Z]+|$)")

func fieldNameToEnvName(s string) string {
	var a []string
	for _, sub := range camel.FindAllStringSubmatch(s, -1) {
		if sub[1] != "" {
			a = append(a, sub[1])
		}
		if sub[2] != "" {
			a = append(a, sub[2])
		}
	}
	return strings.ToUpper(strings.Join(a, "_"))
}

// preparations for docker
func handleEnvironmentVars[T any](config *T) {
	configValue := reflect.Indirect(reflect.ValueOf(config))
	configType := configValue.Type()
	for index := 0; index < configType.NumField(); index++ {
		fieldName := configType.Field(index).Name
		fieldConfig := configType.Field(index).Tag.Get("config")
		envName := fieldNameToEnvName(fieldName)
		envValue := os.Getenv(envName)
		if envValue != "" {
			if !strings.Contains(fieldConfig, "secret") {
				fmt.Println("use environment variable: ", envName, " = ", envValue)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Int64 || configValue.FieldByName(fieldName).Kind() == reflect.Int {
				i, _ := strconv.ParseInt(envValue, 10, 64)
				configValue.FieldByName(fieldName).SetInt(i)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.String {
				configValue.FieldByName(fieldName).SetString(envValue)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Bool {
				b, _ := strconv.ParseBool(envValue)
				configValue.FieldByName(fieldName).SetBool(b)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Float64 {
				f, _ := strconv.ParseFloat(envValue, 64)
				configValue.FieldByName(fieldName).SetFloat(f)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Slice {
				val := []string{}
				for _, element := range strings.Split(envValue, ",") {
					val = append(val, strings.TrimSpace(element))
				}
				configValue.FieldByName(fieldName).Set(reflect.ValueOf(val))
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Map {
				value := map[string]string{}
				for _, element := range strings.Split(envValue, ",") {
					keyVal := strings.Split(element, ":")
					key := strings.TrimSpace(keyVal[0])
					val := strings.TrimSpace(keyVal[1])
					value[key] = val
				}
				configValue.FieldByName(fieldName).Set(reflect.ValueOf(value))
			}
		}
	}
}
