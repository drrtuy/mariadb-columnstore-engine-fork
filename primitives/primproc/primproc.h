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

/******************************************************************************************
 *
 * $Id: primproc.h 2035 2013-01-21 14:12:19Z rdempsey $
 *
 ******************************************************************************************/
/**
 * @file
 */
#pragma once

#include <string>
#include <sstream>
#include <exception>
#include <iostream>
#include <unistd.h>
#include <stdexcept>
#include <boost/thread.hpp>
#include <map>

#include "pp_logger.h"
#include "service.h"

#include "dlpack/dlpack.h"
#include "tvm/driver/driver_api.h"
#include "tvm/ir/expr.h"
#include "tvm/runtime/container/array.h"
#include "tvm/runtime/container/shape_tuple.h"
#include "tvm/runtime/data_type.h"
#include "tvm/runtime/c_runtime_api.h"
#include "tvm/runtime/logging.h"
#include "tvm/runtime/module.h"
#include "tvm/target/target.h"
#include <tvm/te/tensor.h>
#include <tvm/te/schedule.h>
#include <tvm/te/operation.h>

using namespace tvm;
using namespace tvm::runtime;
using namespace tvm::te;

class Opt
{
 public:
  int m_debug;
  bool m_fg;
  Opt(int argc, char* argv[]) : m_debug(0), m_fg(false)
  {
    int c;

    while ((c = getopt(argc, argv, "df")) != EOF)
    {
      switch (c)
      {
        case 'd': m_debug++; break;
        case 'f': m_fg = true; break;
        case '?':
        default: break;
      }
    }
  }
};

class ServicePrimProc : public Service, public Opt
{
 public:
  ServicePrimProc(const Opt& opt) : Service("PrimProc"), Opt(opt)
  {
    auto n = Var("n");
    Array<PrimExpr> shape {n};
    static const std::string targetStr{"llvm -mcpu=skylake-avx512"};
    size_t bitsUsed = 64;

    auto emptyVar = Var("emptyVar", DataType::Int(bitsUsed));
    auto firstFilterVar = Var("firstFilterVar", DataType::Int(bitsUsed));
    auto secFilterVar = Var("secFilterVar", DataType::Int(bitsUsed));
    auto src = placeholder(shape, DataType::Int(bitsUsed), "src");
    // IntImm(DataType::Int(bitsUsed) as an explicit type PrimExpr value
    Tensor firstFilterOut = compute(src->shape, [&src, &firstFilterVar, &emptyVar](tvm::PrimExpr i) {
      return if_then_else(src[i] == emptyVar, src[i], firstFilterVar);
    });
    Tensor secFilterOut = compute(src->shape, [&firstFilterOut, &secFilterVar, &emptyVar](tvm::PrimExpr i) {
      return if_then_else(firstFilterOut[i] == secFilterVar, firstFilterOut[i], emptyVar);
    });
    Tensor ridsOut = compute(src->shape, [&secFilterOut, &emptyVar](tvm::PrimExpr i) {
      return if_then_else(secFilterOut[i] == emptyVar, i, 8192);
    });

    // set schedule
    Schedule s = create_schedule({firstFilterOut->op, secFilterOut->op, ridsOut->op});

    // build a module
    std::unordered_map<Tensor, Buffer> binds;
    auto args = Array<ObjectRef>({src, firstFilterOut, secFilterOut, ridsOut, emptyVar,
      firstFilterVar, secFilterVar});
    auto lowered = LowerSchedule(s, args, "int642filters", binds);
    auto target = Target(targetStr);
    auto targetHost = Target(targetStr);
    funcsCollection_ = build(lowered, target, targetHost);
    hasFuncsCollection_ = true;
  }
  void LogErrno() override
  {
    std::cerr << strerror(errno) << std::endl;
  }
  void ParentLogChildMessage(const std::string& str) override
  {
    std::cout << str << std::endl;
  }
  int Child() override;
  int Run()
  {
    return m_fg ? Child() : RunForking();
  }
  std::atomic_flag& getStartupRaceFlag()
  {
    return startupRaceFlag_;
  }
  bool hasFuncsCollection() const
  {
    return hasFuncsCollection_;
  }
  tvm::runtime::Module& getFuncsCollection()
  {
    return funcsCollection_;
  }
 private:
  // Since C++20 flag's init value is false.
  std::atomic_flag startupRaceFlag_ = ATOMIC_FLAG_INIT;
  bool hasFuncsCollection_ = false;
  tvm::runtime::Module funcsCollection_;
};

namespace primitiveprocessor
{
#define SUMMARY_INFO(message)          \
  if (isDebug(SUMMARY))                \
  {                                    \
    std::cout << message << std::endl; \
  }

#define SUMMARY_INFO2(message1, message2)           \
  if (isDebug(SUMMARY))                             \
  {                                                 \
    std::cout << message1 << message2 << std::endl; \
  }

#define SUMMARY_INFO3(message1, message2, message3)             \
  if (isDebug(SUMMARY))                                         \
  {                                                             \
    std::cout << message1 << message2 << message3 << std::endl; \
  }

#define DETAIL_INFO(message)           \
  if (isDebug(DETAIL))                 \
  {                                    \
    std::cout << message << std::endl; \
  }

#define DETAIL_INFO2(message1, message2)            \
  if (isDebug(DETAIL))                              \
  {                                                 \
    std::cout << message1 << message2 << std::endl; \
  }

#define DETAIL_INFO3(message1, message2, message3)              \
  if (isDebug(DETAIL))                                          \
  {                                                             \
    std::cout << message1 << message2 << message3 << std::endl; \
  }

#define VERBOSE_INFO(message)          \
  if (isDebug(VERBOSE))                \
  {                                    \
    std::cout << message << std::endl; \
  }

#define VERBOSE_INFO2(message1, message2)           \
  if (isDebug(VERBOSE))                             \
  {                                                 \
    std::cout << message1 << message2 << std::endl; \
  }

#define VERBOSE_INFO3(message1, message2, message3)             \
  if (isDebug(VERBOSE))                                         \
  {                                                             \
    std::cout << message1 << message2 << message3 << std::endl; \
  }

enum DebugLevel /** @brief Debug level type enumeration */
{
  NONE = 0,    /** @brief No debug info */
  STATS = 1,   /** @brief stats info */
  SUMMARY = 2, /** @brief Summary level debug info */
  DETAIL = 3,  /** @brief A little detail debug info */
  VERBOSE = 4, /** @brief Detailed debug info */
};

bool isDebug(const DebugLevel level);

const int MAX_BUFFER_SIZE = 32768 * 2;

// message log globals
// const logging::LoggingID lid1(28);
// extern logging::Message msg16;
// extern logging::MessageLog ml1;
// extern boost::mutex logLock;
extern Logger* mlp;

extern ServicePrimProc* globServicePrimProc;
}  // namespace primitiveprocessor
