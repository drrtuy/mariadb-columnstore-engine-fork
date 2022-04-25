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

#include "mcs_basic_types.h"
#include "mcs_decimal.h"
#include "primproc.h"
#include "utils/common/columnwidth.h"
#include "datatypes/mcs_datatype.h"
#include "datatypes/mcs_int128.h"
#include "primitives/linux-port/primitiveprocessor.h"
#include "stats.h"
#include "col1block.h"
#include "col2block.h"
#include "col4block.h"
#include "col8block.h"
#include "col16block.h"
#include "col_float_block.h"
#include "col_double_block.h"
#include "col_neg_float.h"
#include "col_neg_double.h"

using namespace primitives;
using namespace datatypes;
using namespace std;

// If a test crashes check if there is a corresponding literal binary array in
// readBlockFromLiteralArray.

class TVMScanFilterTest : public ::testing::Test
{
 protected:
  PrimitiveProcessor pp;
  uint8_t input[MAXDECIMALWIDTH * BLOCK_SIZE];
  alignas(64) uint8_t output[MAXDECIMALWIDTH * BLOCK_SIZE];
  alignas(64) uint8_t block[MAXDECIMALWIDTH * BLOCK_SIZE];
  alignas(64) uint8_t intermediateResult[MAXDECIMALWIDTH * BLOCK_SIZE];

  uint16_t* rids;
  uint32_t i;
  NewColRequestHeader* in;
  ColResultHeader* out;
  ColArgs* args;
  tvm::runtime::Module tvmModule_;

  void SetUp() override
  {
    memset(input, 0, MAXDECIMALWIDTH * BLOCK_SIZE);
    memset(output, 0, MAXDECIMALWIDTH * BLOCK_SIZE);
    in = reinterpret_cast<NewColRequestHeader*>(input);
    out = reinterpret_cast<ColResultHeader*>(output);
    rids = reinterpret_cast<uint16_t*>(&in[1]);
    args = reinterpret_cast<ColArgs*>(&in[1]);
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
    const std::string funcName{"int642filters"};
    auto lowered = LowerSchedule(s, args, funcName, binds);
    auto target = Target(targetStr);
    auto targetHost = Target(targetStr);

    tvmModule_ = build(lowered, target, targetHost);
    pp.tvmFunc = tvmModule_->GetFunction(funcName);
  }

  uint8_t* readBlockFromFile(const std::string& fileName, uint8_t* block)
  {
    int fd;
    uint32_t i;

    fd = open(fileName.c_str(), O_RDONLY);

    if (fd < 0)
    {
      cerr << "getBlock(): skipping this test; needs the index list file " << fileName << endl;
      return nullptr;
    }

    i = read(fd, block, BLOCK_SIZE);

    if (i <= 0)
    {
      cerr << "getBlock(): Couldn't read the file " << fileName << endl;
      close(fd);
      return nullptr;
    }

    if (i != BLOCK_SIZE)
    {
      cerr << "getBlock(): could not read a whole block" << endl;
      close(fd);
      return nullptr;
    }

    close(fd);
    return block;
  }
  uint8_t* readBlockFromLiteralArray(const std::string& fileName, uint8_t* block)
  {
    if (fileName == std::string("col1block.cdf"))
      return &__col1block_cdf[0];
    else if (fileName == std::string("col2block.cdf"))
      return &__col2block_cdf[0];
    else if (fileName == std::string("col4block.cdf"))
      return &__col4block_cdf[0];
    else if (fileName == std::string("col8block.cdf"))
      return &___bin_col8block_cdf[0];
    else if (fileName == std::string("col16block.cdf"))
      return &___bin_col16block_cdf[0];
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
};

TEST_F(TVMScanFilterTest, ColumnScan8Bytes2EqFiltersRIDOutputBoth)
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

  tmp = 1023;
  args->COP = COMPARE_EQ;
  memcpy(args->val, &tmp, in->colType.DataSize);
  args = reinterpret_cast<ColArgs*>(
      &input[sizeof(NewColRequestHeader) + sizeof(ColArgs) + in->colType.DataSize]);
  args->COP = COMPARE_GT;
  tmp = 1022;
  memcpy(args->val, &tmp, in->colType.DataSize);

  pp.setBlockPtr((int*)readBlockFromLiteralArray("col8block.cdf", block));
  pp.columnScanAndFilter<IntegralType>(in, out, intermediateResult);

  // ASSERT_EQ(out->NVALS, 33);
  cout << "out->NVALS " << out->NVALS << std::endl;

  for (i = 0; i < out->NVALS; i++)
  {
    cout << "i resultRid " << resultRid[i] << " resultVal " << resultVal << std::endl;
    // ASSERT_EQ(resultRid[i], (i < 10 ? i : i - 10 + 1001));
    // ASSERT_EQ(resultVal[i], (i < 10 ? i : i - 10 + 1001));
  }

  // datatypes::TSInt128 expectedMax(&(out->Max));
  // datatypes::TSInt128 expectedMin(&(out->Min));

  // ASSERT_EQ(expectedMax.getValue(), __col8block_cdf_umax);
  // ASSERT_EQ(expectedMin.getValue(), __col8block_cdf_umin);
}