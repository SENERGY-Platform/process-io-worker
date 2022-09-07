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

const SqlCreateShardTable = `CREATE TABLE IF NOT EXISTS Shard (
	Address		VARCHAR(255) PRIMARY KEY
);`

const SqlCreateShardsMappingTable = `CREATE TABLE IF NOT EXISTS ShardsMapping (
	UserId				VARCHAR(255) PRIMARY KEY,
	ShardAddress		VARCHAR(255) REFERENCES Shard(Address)
);`

const SqlSelectShardByUser = `SELECT ShardAddress FROM ShardsMapping WHERE UserId = $1;`

const SqlDeleteUserShard = "DELETE FROM ShardsMapping WHERE UserId = $1;"

const SqlCreateUserShard = "INSERT INTO ShardsMapping (UserId, ShardAddress) VALUES ($1, $2);"

const SqlEnsureShard = `INSERT INTO Shard(Address) VALUES ($1) ON CONFLICT DO NOTHING;`

const SqlShardUserCount = `SELECT COUNT(ShardsMapping.UserId), Shard.Address
	FROM Shard LEFT JOIN ShardsMapping ON Shard.Address = ShardsMapping.ShardAddress
	GROUP BY Shard.Address;`

const SQLListShards = `SELECT Address FROM Shard`
