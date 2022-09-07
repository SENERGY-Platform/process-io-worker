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

package cache

import (
	"encoding/json"
	"errors"
	"github.com/patrickmn/go-cache"
	"log"
	"time"
)

type CacheImpl struct {
	l1                *cache.Cache
	defaultExpiration time.Duration
}

type Item struct {
	Key   string
	Value []byte
}

var ErrNotFound = errors.New("key not found in cache")

func NewCache(expiration time.Duration) *CacheImpl {
	return &CacheImpl{l1: cache.New(expiration, time.Minute), defaultExpiration: expiration}
}

func (this *CacheImpl) get(key string) (value []byte, err error) {
	temp, found := this.l1.Get(key)
	if !found {
		err = ErrNotFound
	} else {
		var ok bool
		value, ok = temp.([]byte)
		if !ok {
			err = errors.New("unable to interprete cache result")
		}
	}
	return
}

func (this *CacheImpl) set(key string, value []byte) {
	this.setWithExpiration(key, value, this.defaultExpiration)
	return
}

func (this *CacheImpl) setWithExpiration(key string, value []byte, expiration time.Duration) {
	if expiration <= 0 {
		expiration = this.defaultExpiration
	}
	this.l1.Set(key, value, expiration)
	return
}

func (this *CacheImpl) Use(key string, getter func() (interface{}, error), result interface{}) (err error) {
	value, err := this.get(key)
	if err == nil {
		err = json.Unmarshal(value, result)
		return
	} else if err != ErrNotFound {
		log.Println("WARNING: err in LocalCache::l1.Get()", err)
	}
	temp, err := getter()
	if err != nil {
		return err
	}
	value, err = json.Marshal(temp)
	if err != nil {
		return err
	}
	this.set(key, value)
	return json.Unmarshal(value, &result)
}

func (this *CacheImpl) UseWithExpirationInResult(key string, getter func() (interface{}, time.Duration, error), result interface{}) (err error) {
	value, err := this.get(key)
	if err == nil {
		err = json.Unmarshal(value, result)
		return
	} else if err != ErrNotFound {
		log.Println("WARNING: err in cache.Get()", err)
	}
	temp, expiration, err := getter()
	if err != nil {
		return err
	}
	value, err = json.Marshal(temp)
	if err != nil {
		return err
	}
	this.setWithExpiration(key, value, expiration)
	return json.Unmarshal(value, &result)
}

func (this *CacheImpl) Invalidate(key string) (err error) {
	this.l1.Delete(key)
	return nil
}
