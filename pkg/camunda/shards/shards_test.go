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

package shards

import (
	"context"
	"github.com/SENERGY-Platform/external-task-worker/lib/camunda/cache"
	"github.com/SENERGY-Platform/external-task-worker/lib/test/docker"
	"reflect"
	"sync"
	"testing"
)

func TestSelectShard(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}

	defer wg.Wait()
	defer cancel()

	pgConn, err := docker.Postgres(ctx, &wg, "test")
	if err != nil {
		t.Error(err)
		return
	}

	s, err := New(pgConn, cache.None)
	if err != nil {
		t.Error(err)
		return
	}

	testSelectShard(s, t)
}

func TestSelectShardWithCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}

	defer wg.Wait()
	defer cancel()

	pgConn, err := docker.Postgres(ctx, &wg, "test")
	if err != nil {
		t.Error(err)
		return
	}

	s, err := New(pgConn, cache.New(nil))
	if err != nil {
		t.Error(err)
		return
	}

	testSelectShard(s, t)

}

func testSelectShard(s *Shards, t *testing.T) {
	t.Run("init shards", testInitShards(s))
	t.Run("check expected count", testCheckCount(s, map[string]int{"shard1": 0, "shard2": 1, "shard3": 2}))
	t.Run("check expected shard selection", testCheckShardSelection(s, "shard1"))
	t.Run("ensure shard for user4", testEnsureShardForUser(s, "user4", "shard1"))
	t.Run("check expected count", testCheckCount(s, map[string]int{"shard1": 1, "shard2": 1, "shard3": 2}))
	t.Run("get shard for user2", testEnsureShardForUser(s, "user2", "shard3"))
	t.Run("set shard for user5", testSetShardForUser(s, "user5", "shard2"))
	t.Run("get shard for user5", testEnsureShardForUser(s, "user5", "shard2"))
	t.Run("check expected count", testCheckCount(s, map[string]int{"shard1": 1, "shard2": 2, "shard3": 2}))
	t.Run("update shard for user2", testSetShardForUser(s, "user2", "shard2"))
	t.Run("get shard for user2 after update", testEnsureShardForUser(s, "user2", "shard2"))
	t.Run("check expected count", testCheckCount(s, map[string]int{"shard1": 1, "shard2": 3, "shard3": 1}))
}

func testSetShardForUser(s *Shards, user string, shard string) func(t *testing.T) {
	return func(t *testing.T) {
		err := s.SetShardForUser(user, shard)
		if err != nil {
			t.Error(err)
			return
		}
	}
}

func testEnsureShardForUser(s *Shards, user string, expectedShardUsed string) func(t *testing.T) {
	return func(t *testing.T) {
		shard, err := s.EnsureShardForUser(user)
		if err != nil {
			t.Error(err)
			return
		}
		if shard != expectedShardUsed {
			t.Error("actual:", shard, "expected:", expectedShardUsed)
			return
		}
	}
}

func testCheckCount(s *Shards, expected map[string]int) func(t *testing.T) {
	return func(t *testing.T) {
		actual, err := getShardUserCount(s.db)
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Error("actual:", actual, "expected:", expected)
			return
		}
	}
}

func testCheckShardSelection(s *Shards, expected string) func(t *testing.T) {
	return func(t *testing.T) {
		actual, err := selectShard(s.db)
		if err != nil {
			t.Error(err)
			return
		}
		if actual != expected {
			t.Error("actual:", actual, "expected:", expected)
			return
		}
	}
}

func testInitShards(s *Shards) func(t *testing.T) {
	return func(t *testing.T) {
		err := s.EnsureShard("shard1")
		if err != nil {
			t.Error(err)
			return
		}
		err = s.EnsureShard("shard2")
		if err != nil {
			t.Error(err)
			return
		}
		err = s.EnsureShard("shard3")
		if err != nil {
			t.Error(err)
			return
		}
		err = s.SetShardForUser("user1", "shard2")
		if err != nil {
			t.Error(err)
			return
		}

		err = s.SetShardForUser("user2", "shard3")
		if err != nil {
			t.Error(err)
			return
		}
		err = s.SetShardForUser("user3", "shard3")
		if err != nil {
			t.Error(err)
			return
		}
	}
}
