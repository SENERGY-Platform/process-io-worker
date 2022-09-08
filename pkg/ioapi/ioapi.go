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

package ioapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/SENERGY-Platform/process-io-worker/pkg/auth"
	"github.com/SENERGY-Platform/process-io-worker/pkg/configuration"
	"github.com/SENERGY-Platform/process-io-worker/pkg/model"
	"io"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"time"
)

func New(config configuration.Config) *IoApi {
	return &IoApi{config: config}
}

type IoApi struct {
	config configuration.Config
}

func (this *IoApi) Bulk(token auth.Token, set []model.BulkSetElement, get []string) (outputs model.BulkResponse, err error) {
	body, err := json.Marshal(map[string]interface{}{
		"set": set,
		"get": get,
	})
	if err != nil {
		return outputs, err
	}
	if this.config.Debug {
		log.Println("DEBUG: bulk", token.GetUserId(), string(body))
	}
	client := http.Client{
		Timeout: 5 * time.Second,
	}
	req, err := http.NewRequest(
		"POST",
		this.config.IoApiUrl+"/bulk",
		bytes.NewBuffer(body),
	)
	if err != nil {
		debug.PrintStack()
		return outputs, err
	}
	req.Header.Set("Authorization", token.Jwt())
	req.Header.Set("X-UserId", token.GetUserId())
	resp, err := client.Do(req)
	if err != nil {
		debug.PrintStack()
		return outputs, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		debug.PrintStack()
		temp, _ := io.ReadAll(resp.Body)
		return outputs, fmt.Errorf("unexpected response: %v, %v", resp.StatusCode, string(temp))
	}

	err = json.NewDecoder(resp.Body).Decode(&outputs)
	return outputs, err
}

func (this *IoApi) Set(token auth.Token, key string, value interface{}) (err error) {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if this.config.Debug {
		log.Println("DEBUG: store", token.GetUserId(), key, string(body))
	}
	client := http.Client{
		Timeout: 5 * time.Second,
	}
	req, err := http.NewRequest(
		"PUT",
		this.config.IoApiUrl+"/values/"+url.PathEscape(key),
		bytes.NewBuffer(body),
	)
	if err != nil {
		debug.PrintStack()
		return err
	}
	req.Header.Set("Authorization", token.Jwt())
	req.Header.Set("X-UserId", token.GetUserId())
	resp, err := client.Do(req)
	if err != nil {
		debug.PrintStack()
		return err
	}
	defer resp.Body.Close()

	temp, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		debug.PrintStack()
		return fmt.Errorf("unexpected response: %v, %v", resp.StatusCode, string(temp))
	}
	return err
}

func (this *IoApi) SetForProcessInstance(token auth.Token, key string, value interface{}, definitionId string, instanceId string) (err error) {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if this.config.Debug {
		log.Println("DEBUG: store", token.GetUserId(), key, string(body))
	}
	client := http.Client{
		Timeout: 5 * time.Second,
	}
	req, err := http.NewRequest(
		"PUT",
		this.config.IoApiUrl+"/process-definitions/"+url.PathEscape(definitionId)+"/process-instances/"+url.PathEscape(instanceId)+"/values/"+url.PathEscape(key),
		bytes.NewBuffer(body),
	)
	if err != nil {
		debug.PrintStack()
		return err
	}
	req.Header.Set("Authorization", token.Jwt())
	req.Header.Set("X-UserId", token.GetUserId())
	resp, err := client.Do(req)
	if err != nil {
		debug.PrintStack()
		return err
	}
	defer resp.Body.Close()

	temp, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		debug.PrintStack()
		return fmt.Errorf("unexpected response: %v, %v", resp.StatusCode, string(temp))
	}
	return err
}

func (this *IoApi) SetForProcessDefinition(token auth.Token, key string, value interface{}, definitionId string) (err error) {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if this.config.Debug {
		log.Println("DEBUG: store", token.GetUserId(), key, string(body))
	}
	client := http.Client{
		Timeout: 5 * time.Second,
	}
	req, err := http.NewRequest(
		"PUT",
		this.config.IoApiUrl+"/process-definitions/"+url.PathEscape(definitionId)+"/values/"+url.PathEscape(key),
		bytes.NewBuffer(body),
	)
	if err != nil {
		debug.PrintStack()
		return err
	}
	req.Header.Set("Authorization", token.Jwt())
	req.Header.Set("X-UserId", token.GetUserId())
	resp, err := client.Do(req)
	if err != nil {
		debug.PrintStack()
		return err
	}
	defer resp.Body.Close()

	temp, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		debug.PrintStack()
		return fmt.Errorf("unexpected response: %v, %v", resp.StatusCode, string(temp))
	}
	return err
}

func (this *IoApi) Get(token auth.Token, key string) (value interface{}, err error) {
	if this.config.Debug {
		log.Println("DEBUG: read", token.GetUserId(), key)
	}
	client := http.Client{
		Timeout: 5 * time.Second,
	}
	req, err := http.NewRequest(
		"GET",
		this.config.IoApiUrl+"/values/"+url.PathEscape(key),
		nil,
	)
	if err != nil {
		debug.PrintStack()
		return value, err
	}
	req.Header.Set("Authorization", token.Jwt())
	req.Header.Set("X-UserId", token.GetUserId())
	resp, err := client.Do(req)
	if err != nil {
		debug.PrintStack()
		return value, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		debug.PrintStack()
		temp, _ := io.ReadAll(resp.Body)
		return value, fmt.Errorf("unexpected response: %v, %v", resp.StatusCode, string(temp))
	}

	err = json.NewDecoder(resp.Body).Decode(&value)
	return value, err
}
