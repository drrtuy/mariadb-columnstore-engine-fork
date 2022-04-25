/* Copyright (C) 2021 MariaDB Corporation

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
#include <algorithm>
#include <cstdint>
#include <memory>
#include <gtest/gtest.h>
#include <benchmark/benchmark.h>

#include "dlpack/dlpack.h"
#include "tvm/driver/driver_api.h"
#include "tvm/ir/expr.h"
#include "tvm/runtime/container/array.h"
#include "tvm/runtime/container/shape_tuple.h"
#include "tvm/runtime/data_type.h"
#include "tvm/runtime/c_runtime_api.h"
#include "tvm/runtime/logging.h"
#include "tvm/target/target.h"
#include <tvm/te/tensor.h>
#include <tvm/te/schedule.h>
#include <tvm/te/operation.h>

using namespace std;
using namespace tvm;
using namespace tvm::runtime;
using namespace tvm::te;

class TVMFilterBenchFixture : public benchmark::Fixture
{
 public:
  void SetUp(benchmark::State& state)
  {
    auto n = Var("n");
    Array<PrimExpr> shape {n};
    static const std::string targetStr{"llvm -mcpu=skylake"};
    size_t bitsUsed = 64;

    auto emptyVar = Var("emptyVar", DataType::Int(bitsUsed));
    auto MinVar = Var("Min", DataType::Int(bitsUsed));
    auto MaxVar = Var("Max", DataType::Int(bitsUsed));
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
    // cerr << lowered << endl;

    auto target = Target(targetStr);
    auto targetHost = Target(targetStr);
    vecFilterMod = build(lowered, target, targetHost);
    vecFilterFunc = vecFilterMod->GetFunction("int642filters");
    // Lanes might affect SIMD used
    int ndim = 1;
    int dtype_code = kDLInt;
    int dtype_bits = bitsUsed;
    int dtype_lanes = 1;
    int device_type = kDLCPU;
    int device_id = 0;
    static int64_t shapeArr[1] = {blockSize};

    srcTensor = new DLTensor();
    srcTensor->device = {static_cast<DLDeviceType>(device_type), device_id};
    srcTensor->dtype = {static_cast<uint8_t>(dtype_code), static_cast<uint8_t>(dtype_bits), static_cast<uint16_t>(dtype_lanes)};
    srcTensor->byte_offset = 0;
    srcTensor->ndim = ndim;
    srcTensor->shape = shapeArr;
    srcTensor->strides = nullptr;
    // alignas(64) static int64_t dataPtr[blockSize+8];
    size_t allocBlockBufSize = blockSize+ 64 / sizeof(int64_t);
    int64_t* dataPtr = new int64_t[blockSize+ 64 / sizeof(int64_t)];
    void * absPtr = reinterpret_cast<void*>(dataPtr);
    std::align(bitsUsed, sizeof(int64_t), absPtr, allocBlockBufSize);
    // std::cout << "SetUp new size " << allocBlockBufSize << hex << " dataPtr " << dataPtr << " absPtr " << absPtr << std::endl;
    srcTensor->data = absPtr;
    for (int i = 0; i < shapeArr[0]; ++i)
    {
      static_cast<int64_t*>(srcTensor->data)[i] = i;
    }

    TVMArrayAlloc(shapeArr, ndim, dtype_code, dtype_bits, dtype_lanes,
                  device_type, device_id, &firstFilterOutTensor);
    TVMArrayAlloc(shapeArr, ndim, dtype_code, dtype_bits, dtype_lanes,
                  device_type, device_id, &secFilterOutTensor);
    // !!!!!!!!!!!!!!
    dtype_bits = 32;
    TVMArrayAlloc(shapeArr, ndim, dtype_code, dtype_bits, dtype_lanes,
                  device_type, device_id, &ridsOutTensor);
  }

  // to avoid gcc compile time warning
  void SetUp(const benchmark::State& state)
  {
    SetUp(const_cast<benchmark::State&>(state));
  }

  constexpr const static size_t blockSize=1024;
  PackedFunc vecFilterFunc;
  Module vecFilterMod;
  DLTensor* srcTensor;
  DLTensor* firstFilterOutTensor;
  DLTensor* secFilterOutTensor;
  DLTensor* ridsOutTensor;
};

BENCHMARK_DEFINE_F(TVMFilterBenchFixture, TVM2filtersInt64)(benchmark::State& state)
{
  for (auto _ : state)
  {
    state.PauseTiming();
    TVMValue emptyVar;
    emptyVar.v_int64 = 0xFFFFFFFE;
    TVMValue firstFilterVar;
    firstFilterVar.v_int64 = 20;
    TVMValue secFilterVar;
    secFilterVar.v_int64 = 20000;
    TVMArgValue emptyVarArg(emptyVar, kTVMArgInt);
    TVMArgValue firstFilterVarArg{firstFilterVar, kTVMArgInt};
    TVMArgValue secFilterVarArg{secFilterVar, kTVMArgInt};
    state.ResumeTiming();
    size_t datasetCardinality = state.range(0);
    // Call
    size_t iters = 0;
    for (size_t i = 0; i < datasetCardinality; i += blockSize)
    {
        vecFilterFunc(srcTensor, firstFilterOutTensor, secFilterOutTensor,
          ridsOutTensor, emptyVarArg, firstFilterVarArg, secFilterVarArg);
          ++iters;
    }
  }
}

BENCHMARK_REGISTER_F(TVMFilterBenchFixture, TVM2filtersInt64)->Arg(1000000)->Arg(8000000)->Arg(30000000)->Arg(50000000)->Arg(75000000)->Arg(100000000);
BENCHMARK_MAIN();
