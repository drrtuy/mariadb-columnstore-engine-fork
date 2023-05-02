/* Copyright (C) 2014 InfiniDB, Inc.

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

/*****************************************************************************
 * $Id: load_brm.cpp 1905 2013-06-14 18:42:28Z rdempsey $
 *
 ****************************************************************************/

#include <unistd.h>

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <stdexcept>
#include <thread>
using namespace std;

#include <gtest/gtest.h>

#include "mastersegmenttable.h"
#include "extentmap.h"
std::mutex m;

namespace BRM
{
class UTTester
{
 public:
  MSTEntry* fEMRBTreeShminfo = nullptr;
  const MasterSegmentTable fMST;
  ExtentMapRBTreeImpl* fPExtMapRBTreeImpl = nullptr;
  ExtentMapRBTree* fExtentMapRBTree = nullptr;
  ShmKeys fShmKeys;

  key_t chooseShmkey(const MSTEntry* masterTableEntry, const uint32_t keyRangeBase) const
  {
    int fixedKeys = 1;

    if (masterTableEntry->tableShmkey + 1 == (key_t)(keyRangeBase + fShmKeys.KEYRANGE_SIZE - 1) ||
        (unsigned)masterTableEntry->tableShmkey < keyRangeBase)
    {
      return keyRangeBase + fixedKeys;
    }
    return masterTableEntry->tableShmkey + 1;
  }

  void growEMShmseg(std::mutex& m, std::condition_variable& cv, const bool w, size_t size = 0)
  {
    size_t allocSize;
    auto newShmKey = chooseShmkey(fEMRBTreeShminfo, fShmKeys.KEYRANGE_EXTENTMAP_BASE);

    if (fEMRBTreeShminfo->allocdSize == 0)
      allocSize = ExtentMap::EM_RB_TREE_INITIAL_SIZE;
    else
      allocSize = ExtentMap::EM_RB_TREE_INCREMENT;

    allocSize = std::max(size, allocSize);

    if (!fPExtMapRBTreeImpl)
    {
      if (fEMRBTreeShminfo->tableShmkey == 0)
        fEMRBTreeShminfo->tableShmkey = newShmKey;

      fPExtMapRBTreeImpl =
          ExtentMapRBTreeImpl::makeExtentMapRBTreeImpl(fEMRBTreeShminfo->tableShmkey, allocSize);
    }
    else
    {
      fEMRBTreeShminfo->tableShmkey = newShmKey;
      fPExtMapRBTreeImpl->grow(fEMRBTreeShminfo->tableShmkey, allocSize);
    }

    fEMRBTreeShminfo->allocdSize += allocSize;
    //  if (w)
    //  {
    //    std::unique_lock lk(m);
    //    cv.wait(lk);
    //  }

    fExtentMapRBTree = fPExtMapRBTreeImpl->get();

    // That's mean we have a initial size.
    if (fEMRBTreeShminfo->currentSize == 0)
      fEMRBTreeShminfo->currentSize = ExtentMap::EM_RB_TREE_EMPTY_SIZE;
  }

  void grab(ExtentMap::OPS op, std::mutex& m, std::condition_variable& cv, const bool w = false)
  {
    boost::mutex::scoped_lock lk(mutex);

    if (op == ExtentMap::READ)
    {
      fEMRBTreeShminfo = fMST.getTable_read(MasterSegmentTable::EMTable);
    }
    else
    {
      fEMRBTreeShminfo = fMST.getTable_write(MasterSegmentTable::EMTable);
    }

    if (!fPExtMapRBTreeImpl || fPExtMapRBTreeImpl->key() != (uint32_t)fEMRBTreeShminfo->tableShmkey)
    {
      if (fEMRBTreeShminfo->allocdSize == 0)
      {
        if (op == ExtentMap::READ)
        {
          fMST.getTable_upgrade(MasterSegmentTable::EMTable);

          if (fEMRBTreeShminfo->allocdSize == 0)
          {
            growEMShmseg(m, cv, w);
          }

          // Has to be done holding the write lock.
          fMST.getTable_downgrade(MasterSegmentTable::EMTable);
        }
        else
        {
          growEMShmseg(m, cv, w);
        }
      }
      else
      {
        fPExtMapRBTreeImpl = ExtentMapRBTreeImpl::makeExtentMapRBTreeImpl(fEMRBTreeShminfo->tableShmkey, 0);

        fExtentMapRBTree = fPExtMapRBTreeImpl->get();
        if (fExtentMapRBTree == nullptr)
        {
          log_errno("ExtentMap cannot create RBTree in shared memory segment");
          throw runtime_error("ExtentMap cannot create RBTree in shared memory segment");
        }
      }
    }
    else
    {
      fExtentMapRBTree = fPExtMapRBTreeImpl->get();
    }
  }

  void run(auto& m, auto& cv)
  {
    std::jthread t1(
        [this, &m, &cv]()
        {
          const bool wait = true;
          grab(BRM::ExtentMap::READ, m, cv, wait);
          //  std::unique_lock<std::mutex> lk(m);
          std::cout << "key in t1 " << this->fPExtMapRBTreeImpl->key() << std::endl;
          //  cv.wait(lk);
          // start lookups
          ASSERT_NE(this->fExtentMapRBTree, nullptr);
        });
    std::jthread t2(
        [this, &m, &cv]()
        {
          const bool doesntWait = false;
          std::this_thread::sleep_for(1000ms);
          std::unique_lock lk(m);
          cv.notify_all();
          grab(BRM::ExtentMap::READ, m, cv, doesntWait);
          //  std::unique_lock<std::mutex> lk(m);
          //  em.fPExtMapRBTreeImpl->grow(em.fPExtMapRBTreeImpl->key(), 32 * 1024 * 1024);
          //  em.grabEMEntryTable(BRM::ExtentMap::READ);
          //  std::cout << "key in t2 " << em.fPExtMapRBTreeImpl->key() << std::endl;
          //  cv.notify_one();
          //  em.grabEMEntryTable(BRM::ExtentMap::READ);
        });
  }

  //   void thread2()
  //   {
  //     em.grabEMEntryTable(ExtentMap::READ);
  //   }
};

}  // namespace BRM

int main(int argc, char** argv)
{
  //   BRM::ExtentMap em;
  std::condition_variable cv;
  BRM::UTTester ut;
  ut.run(m, cv);
  //   auto f1 = [&em]() { std::jthread t1([&em]() { em.grabEMEntryTable(BRM::ExtentMap::READ); }); };
  // std::jthread::jthread t1([&em]() { em::grabEMEntryTable(READ); });
}
