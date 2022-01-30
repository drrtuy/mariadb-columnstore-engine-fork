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

#include <iostream>
#include <gtest/gtest.h>
#include <benchmark/benchmark.h>

#include "datatypes/mcs_datatype.h"
#include "simd_sse.h"
#include "stats.h"
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
using namespace simd;
using namespace std;

#include "tensorflow/c/tf_tensor_internal.h"
#include "tensorflow/cc/ops/const_op.h" 
#include "tensorflow/cc/client/client_session.h" 
#include "tensorflow/cc/ops/math_ops.h"
#include "tensorflow/cc/ops/standard_ops.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/graph/default_device.h"
#include "tensorflow/core/graph/graph_def_builder.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/status.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/lib/core/threadpool.h"
#include "tensorflow/core/lib/io/path.h"
#include "tensorflow/core/lib/strings/stringprintf.h"
#include "tensorflow/core/platform/env.h"
#include "tensorflow/core/platform/init_main.h"
#include "tensorflow/core/platform/logging.h"
#include "tensorflow/core/platform/types.h"
#include "tensorflow/core/public/session.h"
#include "tensorflow/core/util/command_line_flags.h"

using namespace tensorflow;
using namespace tensorflow::ops;
namespace mcs_tf
{
    static auto globTFScope = Scope::NewRootScope();
    static ClientSession globSession(globTFScope);
}

// TODO Use FastOperation() to speed up run loop

uint8_t* readBlockFromLiteralArray(const std::string& fileName)
{
   if (fileName == std::string("col1block.cdf"))
     return &__col1block_cdf[0];
   else if (fileName == std::string("col2block.cdf"))
     return &__col2block_cdf[0];
   else if (fileName == std::string("col4block.cdf"))
     return &__col4block_cdf[0];
   else if (fileName == std::string("col8block.cdf"))
     return &___bin_col8block_cdf[0];
   else if (fileName == std::string("col_float_block.cdf"))
     return &___bin_col_float_block_cdf[0];
   else if (fileName == std::string("col_double_block.cdf"))
     return &___bin_col_double_block_cdf[0];
   else if (fileName == std::string("col_neg_float.cdf"))
     return &___bin_col_neg_float_cdf[0];
   else if (fileName == std::string("col_neg_double.cdf"))
     return &___bin_col_neg_double_cdf[0];

   return nullptr;
}

uint8_t* readBlockFromLiteralArray(const std::string& fileName, uint8_t* block)
{
  return readBlockFromLiteralArray(fileName);
}

int8_t* blockBuf = nullptr;

class FilterBenchFixture : public benchmark::Fixture
{
 public:
  PrimitiveProcessor pp;
  uint8_t input[BLOCK_SIZE];
  uint8_t output[4 * BLOCK_SIZE];
  uint8_t block[BLOCK_SIZE];
  uint16_t* rids;
  uint32_t i;
  uint32_t written;
  NewColRequestHeader* in;
  ColResultHeader* out;
  ColArgs* args;

  void SetUp(benchmark::State& state)
  {
    memset(input, 0, BLOCK_SIZE);
    memset(output, 0, 4 * BLOCK_SIZE);
    in = reinterpret_cast<NewColRequestHeader*>(input);
    out = reinterpret_cast<ColResultHeader*>(output);
    rids = reinterpret_cast<uint16_t*>(&in[1]);
    args = reinterpret_cast<ColArgs*>(&in[1]);
    const size_t ExtentRows = 200000000;
    if (!blockBuf)
      blockBuf = createAndFillBuffer<int8_t>(ExtentRows);
  }

  // to avoid gcc compile time warning
  void SetUp(const benchmark::State& state)
  {
    SetUp(const_cast<benchmark::State&>(state));
  }

  void inTestRunSetUp(const std::string& dataName, const size_t dataSize,
    const uint8_t dataType, const uint32_t outputType, ColArgs* args)
  {
    in->colType = ColRequestHeaderDataType();
    in->colType.DataSize = dataSize;
    in->colType.DataType = dataType;
    in->OutputType = outputType;
    in->NOPS = 0;
    in->NVALS = 0;
    pp.setBlockPtr((int*) readBlockFromLiteralArray(dataName, block));
  }
  void runFilterBenchLegacy()
  {
    pp.p_Col(in, out, 4 * BLOCK_SIZE, &written);
  }

  template<int W>
  void runFilterBenchTemplated()
  {
    using IntegralType = typename datatypes::WidthToSIntegralType<W>::type;
    pp.columnScanAndFilter<IntegralType>(in, out);
  }

  template<int W>
  void setUp1EqFilter()
  {
    using IntegralType = typename datatypes::WidthToSIntegralType<W>::type;
    in->NOPS = 1;
    in->NVALS = 0;
    IntegralType tmp = 20;
    args->COP = COMPARE_EQ;
    memcpy(args->val, &tmp, W);
  }
  
  template<typename T>
  T* createAndFillBuffer(const size_t size)
  {
    T invValuesArray[16] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    vi128_t initialIncVector = _mm_loadu_si128(reinterpret_cast<vi128_t*>(&invValuesArray[0]));
    vi128_t values = _mm_set1_epi8(0x00);
    
    T* buf = new T[size * sizeof(T)];
    std::cout << "createAndFillBuff size " << size << std::endl;
    for (size_t i = 0; i < size; i += sizeof(vi128_t)/sizeof(T))
    {
      //std::cout << "createAndFillBuff i " << i << std::endl;
      vi128_t shuffleMask = _mm_set1_epi8(0x0F);
      vi128_t incVector = _mm_shuffle_epi8(initialIncVector, shuffleMask);
      values = _mm_add_epi8(values, incVector);
      _mm_storeu_si128(reinterpret_cast<vi128_t*>(&buf[i]), values);
    }
    return buf;
  }
};

BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan1ByteVectorized)(benchmark::State& state)
{
  for (auto _ : state)
  {
    constexpr const uint8_t W = 1;
    const size_t setSize = state.range(0);
    //using T = datatypes::WidthToSIntegralType<W>::type;
    state.PauseTiming();
    inTestRunSetUp("col1block.cdf", W, SystemCatalog::TINYINT, OT_DATAVALUE, args);
    state.ResumeTiming();
    for (size_t i = 0; i < setSize; i += BLOCK_SIZE / W)
    {
      pp.setBlockPtr(reinterpret_cast<int*>(blockBuf + i));
      runFilterBenchTemplated<W>();
    }
  }
}

BENCHMARK_REGISTER_F(FilterBenchFixture, BM_ColumnScan1ByteVectorized)->Arg(1000000)->Arg(8000000)->Arg(30000000)->Arg(50000000)->Arg(75000000)->Arg(100000000);

BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan1Byte1FilterScalar)(benchmark::State& state)
{
  for (auto _ : state)
  {
    state.PauseTiming();
    constexpr const uint8_t W = 1;
    const size_t setSize = state.range(0);
    setUp1EqFilter<W>();
    inTestRunSetUp("col1block.cdf", W, SystemCatalog::CHAR, OT_DATAVALUE, args);
    state.ResumeTiming();
    for (size_t i = 0; i < setSize; i += BLOCK_SIZE / W)
    {
      pp.setBlockPtr(reinterpret_cast<int*>(blockBuf + i));
      runFilterBenchTemplated<W>();
    }

//    runFilterBenchTemplated<W>();
  }
}

BENCHMARK_REGISTER_F(FilterBenchFixture, BM_ColumnScan1Byte1FilterScalar)->Arg(1000000)->Arg(8000000)->Arg(30000000)->Arg(50000000)->Arg(75000000)->Arg(100000000);

BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan8Byte2FiltersVectorized)(benchmark::State& state)
{
  for (auto _ : state)
  {
    state.PauseTiming();
    constexpr const uint8_t W = 1;
    const size_t setSize = state.range(0);
    setUp1EqFilter<W>();
    inTestRunSetUp("col1block.cdf", W, SystemCatalog::BIGINT, OT_BOTH, args);
    state.ResumeTiming();
    for (size_t i = 0; i < setSize; i += BLOCK_SIZE / W)
    {
      pp.setBlockPtr(reinterpret_cast<int*>(blockBuf + i));
      runFilterBenchTemplated<W>();
    }

//    runFilterBenchTemplated<W>();
  }
}

BENCHMARK_REGISTER_F(FilterBenchFixture, BM_ColumnScan8Byte2FiltersVectorized)->Arg(1000000)->Arg(8000000)->Arg(30000000)->Arg(50000000)->Arg(75000000)->Arg(100000000);

TEST_F(ColumnScanFilterTest, ColumnScan8Bytes2EqFiltersRIDOutputBoth)
{
  constexpr const uint8_t W = 8;
  using IntegralType = datatypes::WidthToSIntegralType<W>::type;
  IntegralType tmp;
  IntegralType* resultVal = getValuesArrayPosition<IntegralType>(getFirstValueArrayPosition(out), 0);
  primitives::RIDType* resultRid = getRIDArrayPosition(getFirstRIDArrayPosition(out), 0);

  in->colType.DataSize = W;
  in->colType.DataType = SystemCatalog::INT;
  in->OutputType = OT_BOTH;
  in->NOPS = 2;
  in->BOP = BOP_OR;
  in->NVALS = 0;
  
  tmp = 10;
  args->COP = COMPARE_LT;
  memcpy(args->val, &tmp, in->colType.DataSize);
  args = reinterpret_cast<ColArgs*>(&input[sizeof(NewColRequestHeader) +
                                           sizeof(ColArgs) + in->colType.DataSize]);
  args->COP = COMPARE_GT;
  tmp = 1000;
  memcpy(args->val, &tmp, in->colType.DataSize);

  pp.setBlockPtr((int*) readBlockFromLiteralArray("col8block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out);

  ASSERT_EQ(out->NVALS, 33);

  for (i = 0; i < out->NVALS; i++)
  {
      ASSERT_EQ(resultRid[i], (i < 10 ? i : i - 10 + 1001));
      ASSERT_EQ(resultVal[i], (i < 10 ? i : i - 10 + 1001));
  }
  ASSERT_EQ(out->Max, __col8block_cdf_umax);
  ASSERT_EQ(out->Min, __col8block_cdf_umin);
}

void simpleTenzorFilter(Tensor& x, Tensor& y, std::vector<Tensor>& r)
{
/*
  auto input = Tensor(DataType::DT_UINT8, {8192});
  for (size_t i = 0; i < 8192; ++i)
      input.flat<uint8_t>()(i) = srcArray[i];

  auto filter = Tensor(DataType::DT_UINT8, {8192}); // Initializer
  for (size_t i = 0; i < 8192; ++i)
      input.flat<uint8_t>()(i) = joblist::UTINYINTEMPTYROW;
*/
  auto eq_op = Equal(mcs_tf::globTFScope, x, y); 
  TF_CHECK_OK(mcs_tf::globSession.Run({eq_op}, &r));
    //std::cout << "Underlying Scalar value -> " << outputs[0].flat<bool>() << std::endl;
}

/*
BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan1ByteTensorizedCode)(benchmark::State& state)
{
  for (auto _ : state)
  {
    state.PauseTiming();
    const size_t setSize = state.range(0);
    constexpr const uint8_t W = 1;
    using T = datatypes::WidthToSIntegralType<W>::type;
    // Hardcoded type
    auto input = Tensor(DataType::DT_INT8, {BLOCK_SIZE});
    std::vector<Tensor> r;
    // Hardcoded type
    auto filter = Tensor(DataType::DT_INT8, {BLOCK_SIZE}); // Initializer
    for (size_t i = 0; i < BLOCK_SIZE; ++i)
        input.flat<T>()(i) = joblist::UTINYINTEMPTYROW;

    for (size_t i = 0; i < setSize; i += BLOCK_SIZE / W)
    {
      for (size_t i = 0; i < BLOCK_SIZE; ++i)
          input.flat<T>()(i) = blockBuf[i];

      state.ResumeTiming();
      simpleTenzorFilter(input, filter, r);
      state.PauseTiming();
    }
  }
}
*/

BENCHMARK_DEFINE_F(FilterBenchFixture, BM_ColumnScan1ByteTensorizedCode)(benchmark::State& state)
{
  for (auto _ : state)
  {
    state.PauseTiming();
    const size_t setSize = state.range(0);
    constexpr const uint8_t W = 1;
    using T = datatypes::WidthToSIntegralType<W>::type;
    // Hardcoded type
    auto input = Tensor(DataType::DT_INT8, {setSize});
    std::vector<Tensor> r;
    // Hardcoded type
    auto filter = Tensor(DataType::DT_INT8, {setSize}); // Initializer
    for (size_t i = 0; i < setSize; ++i)
        input.flat<T>()(i) = joblist::UTINYINTEMPTYROW;

    for (size_t i = 0; i < setSize; ++i)
        input.flat<T>()(i) = blockBuf[i];

    state.ResumeTiming();
    simpleTenzorFilter(input, filter, r);
    state.PauseTiming();
  }
}

BENCHMARK_REGISTER_F(FilterBenchFixture, BM_ColumnScan1ByteTensorizedCode)->Arg(1000000)->Arg(8000000)->Arg(30000000)->Arg(50000000)->Arg(75000000)->Arg(100000000);

BENCHMARK_MAIN();
// vim:ts=2 sw=2:
