/* Copyright (C) 2024 MariaDB Corporation

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

#pragma once

#include <string>
#include <iostream>
#include <thread>
#include <memory>

// https://apple.github.io/foundationdb/api-c.html
// We have to define `FDB_API_VERSION` before include `fdb_c.h` header.
#define FDB_API_VERSION 630
#include <foundationdb/fdb_c.h>

namespace FDBCS
{
// TODO: How about uint8_t.
using ByteArray = std::string;

// Represensts a `transaction`.
class Transaction
{
 public:
  Transaction() = delete;
  Transaction(const Transaction&) = delete;
  Transaction(Transaction&&) = delete;
  Transaction& operator=(const Transaction&) = delete;
  Transaction& operator=(Transaction&&) = delete;
  explicit Transaction(FDBTransaction* tnx);
  ~Transaction();

  // Sets a given `key` and given `value`.
  void set(const ByteArray& key, const ByteArray& value) const;
  // Gets a `value` by the given `key`.
  std::pair<bool, ByteArray> get(const ByteArray& key) const;
  // Removes a given `key` from database.
  void remove(const ByteArray& key) const;
  // Removes all keys in the given range, starting from `beginKey` until `endKey`, but not including `endKey`.
  void removeRange(const ByteArray& beginKey, const ByteArray& endKey) const;
  // Commits transaction.
  bool commit() const;

 private:
  FDBTransaction* tnx_{nullptr};
};

// Represents network class.
class FDBNetwork
{
 public:
  FDBNetwork() = default;
  FDBNetwork(const FDBNetwork&) = delete;
  FDBNetwork(FDBNetwork&&) = delete;
  FDBNetwork& operator=(const FDBNetwork&) = delete;
  FDBNetwork& operator=(FDBNetwork&&) = delete;
  ~FDBNetwork();

  bool setUpAndRunNetwork();

 private:
  std::thread netThread;
};

// Represents database class.
class FDBDataBase
{
 public:
  FDBDataBase() = delete;
  FDBDataBase(const FDBDataBase&) = delete;
  FDBDataBase& operator=(FDBDataBase&) = delete;
  FDBDataBase(FDBDataBase&&) = delete;
  FDBDataBase& operator=(FDBDataBase&&) = delete;
  explicit FDBDataBase(FDBDatabase* database);
  ~FDBDataBase();

  std::unique_ptr<Transaction> createTransaction() const;
  bool isDataBaseReady() const;

 private:
  FDBDatabase* database_;
  const uint32_t secondsToWait_ = 3;
};

// Represents a creator class for the `FDBDatabase`.
class DataBaseCreator
{
 public:
  // Creates a `FDBDataBase` from the given `clusterFilePath` (path to the cluster file).
  static std::shared_ptr<FDBDataBase> createDataBase(const std::string clusterFilePath);
};

bool setAPIVersion();
}  // namespace FDBCS
