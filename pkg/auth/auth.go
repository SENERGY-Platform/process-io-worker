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
	"github.com/SENERGY-Platform/process-io-worker/pkg/cache"
	"github.com/SENERGY-Platform/process-io-worker/pkg/configuration"
	"time"
)

type Auth struct {
	config configuration.Config
	cache  cache.Cache
	openid *OpenidToken
}

func New(config configuration.Config) *Auth {
	return &Auth{config: config, cache: cache.NewCache(time.Duration(config.TokenCacheDefaultExpirationInSeconds) * time.Second)}
}

var TimeNow = func() time.Time {
	return time.Now()
}
