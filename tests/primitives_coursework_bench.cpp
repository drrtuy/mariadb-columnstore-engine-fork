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

#include <cstdint>
#include <iostream>
#include <gtest/gtest.h>
#include <benchmark/benchmark.h>

// #include "datatypes/mcs_datatype.h"
// #include "stats.h"
// #include "primitivemsg.h"
#include "primitives/linux-port/primitiveprocessor.h"
#include "col1block.h"
#include "col2block.h"
#include "col4block.h"
#include "col8block.h"
#include "col_float_block.h"
#include "col_double_block.h"
#include "col_neg_float.h"
#include "col_neg_double.h"

using namespace primitives;
using namespace datatypes;
using namespace std;

// TODO Use FastOperation() to speed up run loop

uint8_t* readBlockFromLiteralArray(const std::string& fileName, uint8_t* block)
{
  void* buf = nullptr;
  size_t size = 0;
  if (fileName == std::string("col1block.cdf"))
  {
    buf = &__col1block_cdf[0];
    size = __col1block_cdf_len;
  }
  else if (fileName == std::string("col2block.cdf"))
  {
    buf = &__col2block_cdf[0];
    size = __col2block_cdf_len;
  }
  else if (fileName == std::string("col4block.cdf"))
  {
    buf = &__col4block_cdf[0];
    size = __col4block_cdf_len;
  }
  else if (fileName == std::string("col8block.cdf"))
  {
    buf = &___bin_col8block_cdf[0];
    size = ___bin_col8block_cdf_len;
  }
  else if (fileName == std::string("col_float_block.cdf"))
  {
    buf = &___bin_col_float_block_cdf[0];
    size = ___bin_col8block_cdf_len;
  }
  else if (fileName == std::string("col_double_block.cdf"))
  {
    buf = &___bin_col_double_block_cdf[0];
    size = ___bin_col_double_block_cdf_len;
  }
  else if (fileName == std::string("col_neg_float.cdf"))
  {
    buf = &___bin_col_neg_float_cdf[0];
    size = ___bin_col_neg_float_cdf_len;
  }
  else if (fileName == std::string("col_neg_double.cdf"))
  {
    buf = &___bin_col_neg_double_cdf[0];
    size = ___bin_col_neg_double_cdf_len;
  }
  memcpy((void*)block, buf, size);
  return block;

  return nullptr;
}

class FilterBenchFixture : public benchmark::Fixture
{
 public:
  PrimitiveProcessor pp;
  alignas(64) uint8_t input[MAXDECIMALWIDTH * BLOCK_SIZE];
  alignas(64) uint8_t output[MAXDECIMALWIDTH * BLOCK_SIZE];
  alignas(64) uint8_t block[MAXDECIMALWIDTH * BLOCK_SIZE];
  alignas(64) uint8_t intermediateResult[MAXDECIMALWIDTH * BLOCK_SIZE];
  uint16_t* rids;
  uint32_t i;
  uint32_t written;
  NewColRequestHeader* in;
  ColResultHeader* out;
  ColArgs* args;
  PackedFunc vecFilterFunc;

  void SetUp(benchmark::State& state)
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
    auto compileArgs = Array<ObjectRef>({src, firstFilterOut, secFilterOut, ridsOut, emptyVar,
      firstFilterVar, secFilterVar});
    auto lowered = LowerSchedule(s, compileArgs, "int642filters", binds);
    auto target = Target(targetStr);
    auto targetHost = Target(targetStr);
    auto filterMod = build(lowered, target, targetHost);
    vecFilterFunc = filterMod->GetFunction("int642filters");

    memset(input, 0, BLOCK_SIZE);
    memset(output, 0, 4 * BLOCK_SIZE);
    in = reinterpret_cast<NewColRequestHeader*>(input);
    out = reinterpret_cast<ColResultHeader*>(output);
    rids = reinterpret_cast<uint16_t*>(&in[1]);
    args = reinterpret_cast<ColArgs*>(&in[1]);
  }

  // to avoid gcc compile time warning
  void SetUp(const benchmark::State& state)
  {
    SetUp(const_cast<benchmark::State&>(state));
  }

  void inTestRunSetUp(const std::string& dataName, const size_t dataSize, const uint8_t dataType,
                      const uint32_t outputType, ColArgs* args)
  {
    in->colType = ColRequestHeaderDataType();
    in->colType.DataSize = dataSize;
    in->colType.DataType = dataType;
    in->OutputType = outputType;
    in->NOPS = 0;
    in->NVALS = 0;
    pp.setBlockPtr((int*)readBlockFromLiteralArray(dataName, block));
  }

  template <int W>
  void runFilterBenchTemplated()
  {
    using IntegralType = typename datatypes::WidthToSIntegralType<W>::type;
    pp.columnScanAndFilter<IntegralType>(in, out, intermediateResult, &vecFilterFunc);
  }

  template <int W>
  void setUpAnother1EqFilter(const uint8_t bop)
  {
    using IntegralType = typename datatypes::WidthToSIntegralType<W>::type;
    in->NOPS++;
    in->BOP = bop;
    assert(in->NOPS < 1023);
    in->NVALS = 0;
    IntegralType tmp = 1023 - in->NOPS;

    args->COP = COMPARE_EQ;
    memcpy(args->val, &tmp, W);
    args = reinterpret_cast<ColArgs*>(&input[sizeof(NewColRequestHeader) + sizeof(ColArgs) + in->colType.DataSize]);
  }

  void externalJITFrameworkFilter(uint8_t* tempBufPtr, uint8_t* srcArray, uint8_t* out, PackedFunc* tvmFunc)
  {
    size_t bitsUsed = 64;
    // Lanes might affect SIMD used
    int ndim = 1;
    int dtype_code = kDLInt;
    int dtype_bits = bitsUsed;
    int dtype_lanes = 1;
    int device_type = kDLCPU;
    int device_id = 0;
    static int64_t shapeArr[1] = {BLOCK_SIZE};

    void* dstArray = (void*) out;
    void* ridDstArray = (void*)(out + 1024 * 8);
    void* absSrcArray = (void*)srcArray;

    // Move this to filter?
    DLDevice device {static_cast<DLDeviceType>(device_type), device_id};
    DLDataType dtype {static_cast<uint8_t>(dtype_code), static_cast<uint8_t>(dtype_bits), static_cast<uint16_t>(dtype_lanes)};
    DLTensor srcTensor;
    srcTensor.device = device;
    srcTensor.dtype = dtype;
    srcTensor.byte_offset = 0;
    srcTensor.ndim = ndim;
    srcTensor.shape = shapeArr;
    srcTensor.strides = nullptr;
    srcTensor.data = absSrcArray;

    DLTensor firstFilterOutTensor;
    firstFilterOutTensor.device = device;
    firstFilterOutTensor.dtype = dtype;
    firstFilterOutTensor.byte_offset = 0;
    firstFilterOutTensor.ndim = ndim;
    firstFilterOutTensor.shape = shapeArr;
    firstFilterOutTensor.strides = nullptr;
    firstFilterOutTensor.data = tempBufPtr;

    DLTensor secFilterOutTensor;
    secFilterOutTensor.device = device;
    secFilterOutTensor.dtype = dtype;
    secFilterOutTensor.byte_offset = 0;
    secFilterOutTensor.ndim = ndim;
    secFilterOutTensor.shape = shapeArr;
    secFilterOutTensor.strides = nullptr;
    secFilterOutTensor.data = dstArray;

    DLDataType dtype32 {static_cast<uint8_t>(dtype_code), static_cast<uint8_t>(32), static_cast<uint16_t>(dtype_lanes)};
    DLTensor ridsOutTensor;
    ridsOutTensor.device = device;
    ridsOutTensor.dtype = dtype32;
    ridsOutTensor.byte_offset = 0;
    ridsOutTensor.ndim = ndim;
    ridsOutTensor.shape = shapeArr;
    ridsOutTensor.strides = nullptr;
    ridsOutTensor.data = ridDstArray;

    TVMValue emptyVar;
    emptyVar.v_int64 = joblist::BIGINTEMPTYROW;
    TVMValue firstFilterVar;
    firstFilterVar.v_int64 = 11;
    TVMValue secFilterVar;
    secFilterVar.v_int64 = 42;
    TVMArgValue emptyVarArg(emptyVar, kTVMArgInt);
    TVMArgValue firstFilterVarArg{firstFilterVar, kTVMArgInt};
    TVMArgValue secFilterVarArg{secFilterVar, kTVMArgInt};

    (*tvmFunc)(&srcTensor, &firstFilterOutTensor, &secFilterOutTensor, &ridsOutTensor,
      emptyVarArg, firstFilterVarArg, secFilterVarArg);
  }

};

BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan8ByteVectorizedPath)(benchmark::State& state)
{
  for (auto _ : state)
  {
    constexpr const uint8_t W = 8;
    [[maybe_unused]] constexpr int batchSize = BLOCK_SIZE / 8;
    state.PauseTiming();
    inTestRunSetUp("col8block.cdf", W, SystemCatalog::BIGINT, OT_DATAVALUE, args);
    setUpAnother1EqFilter<W>(BOP_AND);
    setUpAnother1EqFilter<W>(BOP_AND);
    state.ResumeTiming();
    // Call
    [[maybe_unused]] const size_t dataSetCardinality = state.range(0);
    for (size_t i = 0; i < dataSetCardinality; i += batchSize)
    {
      runFilterBenchTemplated<W>();
    }
  }
}

BENCHMARK_REGISTER_F(FilterBenchFixture, BM_ColumnScan8ByteVectorizedPath)->Iterations(10)->Arg(1000000)->Arg(8000000)->Arg(30000000)->Arg(50000000)->Arg(75000000)->Arg(100000000);

BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan8ByteTVMPath)(benchmark::State& state)
{

  for (auto _ : state)
  {
    constexpr const uint8_t W = 8;
    [[maybe_unused]] constexpr int batchSize = BLOCK_SIZE / 8;
    state.PauseTiming();
    inTestRunSetUp("col8block.cdf", W, SystemCatalog::BIGINT, OT_DATAVALUE, args);
    setUpAnother1EqFilter<W>(BOP_OR);
    setUpAnother1EqFilter<W>(BOP_OR);
    state.ResumeTiming();
    // Call
    [[maybe_unused]] const size_t dataSetCardinality = state.range(0);
    for (size_t i = 0; i < dataSetCardinality; i += batchSize)
    {
      runFilterBenchTemplated<W>();
    }
  }
}
BENCHMARK_REGISTER_F(FilterBenchFixture, BM_ColumnScan8ByteTVMPath)->Iterations(10)->Arg(1000000)->Arg(8000000)->Arg(30000000)->Arg(50000000)->Arg(75000000)->Arg(100000000);

BENCHMARK_MAIN();
