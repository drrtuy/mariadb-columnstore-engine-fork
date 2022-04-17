/* Copyright (c) 2022 MariaDB Corporation

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
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>
#include <atomic>
#include <queue>
#include <unordered_map>
#include <list>
#include <functional>

#include "primitives/primproc/umsocketselector.h"
#include "prioritythreadpool.h"

namespace threadpool
{
class FairThreadPool
{
 public:
  using Functor = PriorityThreadPool::Functor;

  using TransactionIdxT = uint32_t;
  struct Job
  {
    Job() : weight_(1), priority_(0), id_(0)
    {
    }
    Job(const uint32_t uniqueID, const uint32_t stepID, const TransactionIdxT txnIdx,
        const boost::shared_ptr<Functor>& functor, const primitiveprocessor::SP_UM_IOSOCK& sock,
        const uint32_t weight = 0, const uint32_t priority = 0, const uint32_t id = 0)
     : uniqueID_(uniqueID)
     , stepID_(stepID)
     , txnIdx_(txnIdx)
     , functor_(functor)
     , sock_(sock)
     , weight_(weight)
     , priority_(priority)
     , id_(id)
    {
    }
    uint32_t uniqueID_;
    uint32_t stepID_;
    TransactionIdxT txnIdx_;
    boost::shared_ptr<Functor> functor_;
    primitiveprocessor::SP_UM_IOSOCK sock_;
    uint32_t weight_;
    uint32_t priority_;
    uint32_t id_;
  };

  enum Priority
  {
    LOW,
    MEDIUM,
    HIGH,
    _COUNT,
    EXTRA  // After _COUNT because _COUNT is for jobQueue size and EXTRA isn't a jobQueue. But we need EXTRA
           // in places where Priority is used.
  };

  /*********************************************
   *  ctor/dtor
   *
   *********************************************/

  /** @brief ctor
   */

  FairThreadPool(uint targetWeightPerRun, uint highThreads, uint midThreads, uint lowThreads, uint id = 0);
  virtual ~FairThreadPool();

  void removeJobs(uint32_t id);
  void addJob(const Job& job, bool useLock = true);
  void stop();

  /** @brief for use in debugging
   */
  void dump();

  // If a job is blocked, we want to temporarily increase the number of threads managed by the pool
  // A problem can occur if all threads are running long or blocked for a single query. Other
  // queries won't get serviced, even though there are cpu cycles available.
  // These calls are currently protected by respondLock in sendThread(). If you call from other
  // places, you need to consider atomicity.
  void incBlockedThreads()
  {
    blockedThreads++;
  }
  void decBlockedThreads()
  {
    blockedThreads--;
  }
  uint32_t blockedThreadCount()
  {
    return blockedThreads;
  }

 protected:
 private:
  struct ThreadHelper
  {
    ThreadHelper(FairThreadPool* impl, Priority queue) : ptp(impl), preferredQueue(queue)
    {
    }
    void operator()()
    {
      ptp->threadFcn(preferredQueue);
    }
    FairThreadPool* ptp;
    Priority preferredQueue;
  };

  explicit FairThreadPool();
  explicit FairThreadPool(const FairThreadPool&);
  FairThreadPool& operator=(const FairThreadPool&);

  Priority pickAQueue(Priority preference);
  void threadFcn(const Priority preferredQueue);
  void sendErrorMsg(uint32_t id, uint32_t step, primitiveprocessor::SP_UM_IOSOCK sock);

  std::list<Job> jobQueues[3];  // higher indexes = higher priority_
  uint32_t threadCounts[3];
  uint32_t defaultThreadCounts[3];
  boost::mutex mutex;
  boost::condition newJob;
  boost::thread_group threads;
  bool _stop;
  uint32_t weightPerRun;
  volatile uint id;  // prevent it from being optimized out

  // WIP
  using WeightT = uint32_t;
  using WeightedTxnT = std::pair<WeightT, TransactionIdxT>;
  using WeightedTxnVec = std::vector<WeightedTxnT>;
  struct PrioQueueCmp
  {
    bool operator()(WeightedTxnT lhs, WeightedTxnT rhs)
    {
      return lhs.first < rhs.first;
    }
  };
  using RunListT = std::vector<Job>;
  using RescheduleVecType = std::vector<bool>;
  using WeightedTxnPrioQueue = std::priority_queue<WeightedTxnT, WeightedTxnVec, PrioQueueCmp>;
  using ThreadPoolJobsList = std::list<Job>;
  using Txn2ThreadPoolJobsListMap = std::unordered_map<TransactionIdxT, ThreadPoolJobsList*>;
  Txn2ThreadPoolJobsListMap txn2JobsListMap_;
  WeightedTxnPrioQueue weightedTxnsQueue_;
  std::atomic<uint32_t> blockedThreads;
  std::atomic<uint32_t> extraThreads;
  bool stopExtra;
};

}  // namespace threadpool