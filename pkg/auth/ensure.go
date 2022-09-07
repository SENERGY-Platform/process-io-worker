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

package auth

import (
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/process-io-worker/pkg/configuration"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

func (this *Auth) Ensure() (token Token, err error) {
	if this.openid == nil {
		this.openid = &OpenidToken{}
	}
	duration := TimeNow().Sub(this.openid.RequestTime).Seconds()

	// subtract 5 seconds from expiration as a buffer
	buffer := 10.0

	if this.openid.AccessToken != "" && this.openid.ExpiresIn-buffer > duration {
		return this.openid.ParsedToken, nil
	}

	// subtract 5 seconds from expiration as a buffer
	if this.openid.RefreshToken != "" && this.openid.RefreshExpiresIn-buffer > duration {
		log.Println("refresh token", this.openid.RefreshExpiresIn, duration)
		err = refreshOpenidToken(this.openid, this.config)
		if err != nil {
			log.Println("WARNING: unable to use refresh-token", err)
		} else {
			return this.openid.ParsedToken, nil
		}
	}

	log.Println("get new access token")
	err = getOpenidToken(this.openid, this.config)
	if err != nil {
		log.Println("ERROR: unable to get new access token", err)
		this.openid = &OpenidToken{}
	}
	return this.openid.ParsedToken, nil
}

func getOpenidToken(token *OpenidToken, config configuration.Config) (err error) {
	requesttime := TimeNow()
	resp, err := http.PostForm(config.AuthEndpoint+"/auth/realms/master/protocol/openid-connect/token", url.Values{
		"client_id":     {config.AuthClientId},
		"client_secret": {config.AuthClientSecret},
		"grant_type":    {"client_credentials"},
	})

	if err != nil {
		log.Println("ERROR: getOpenidToken::PostForm()", err)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Println("ERROR: getOpenidToken()", resp.StatusCode, string(body))
		err = errors.New("access denied")
		resp.Body.Close()
		return
	}
	err = json.NewDecoder(resp.Body).Decode(token)
	if err != nil {
		return err
	}
	token.RequestTime = requesttime
	token.ParsedToken, err = Parse(token.AccessToken)
	return
}

func refreshOpenidToken(token *OpenidToken, config configuration.Config) (err error) {
	requesttime := TimeNow()
	resp, err := http.PostForm(config.AuthEndpoint+"/auth/realms/master/protocol/openid-connect/token", url.Values{
		"client_id":     {config.AuthClientId},
		"client_secret": {config.AuthClientSecret},
		"refresh_token": {token.RefreshToken},
		"grant_type":    {"refresh_token"},
	})

	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Println("ERROR: refreshOpenidToken()", resp.StatusCode, string(body))
		err = errors.New("access denied")
		resp.Body.Close()
		return
	}
	err = json.NewDecoder(resp.Body).Decode(token)
	if err != nil {
		return err
	}
	token.RequestTime = requesttime
	token.ParsedToken, err = Parse(token.AccessToken)
	return
}
