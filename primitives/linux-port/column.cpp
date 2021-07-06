/* Copyright (C) 2014 InfiniDB, Inc.
   Copyright (C) 2016-2021 MariaDB Corporation

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
 * $Id: column.cpp 2103 2013-06-04 17:53:38Z dcathey $
 *
 ****************************************************************************/
#include <iostream>
#include <sstream>
//#define NDEBUG
#include <cassert>
#include <cmath>
#ifndef _MSC_VER
#include <pthread.h>
#else
#endif
using namespace std;

#include <boost/scoped_array.hpp>
using namespace boost;

#include "primitiveprocessor.h"
#include "messagelog.h"
#include "messageobj.h"
#include "we_type.h"
#include "stats.h"
#include "primproc.h"
#include "dataconvert.h"
#include "mcs_decimal.h"

using namespace logging;
using namespace dbbc;
using namespace primitives;
using namespace primitiveprocessor;
using namespace execplan;



// #########################################

namespace
{
using RID_T = uint16_t;  // Row index type, as used in rid arrays

// Column filtering is dispatched 4-way based on the column type,
// which defines implementation of comparison operations for the column values
enum ENUM_KIND {KIND_DEFAULT,   // compared as signed integers
                KIND_UNSIGNED,  // compared as unsigned integers
                KIND_FLOAT,     // compared as floating-point numbers
                KIND_TEXT};     // whitespace-trimmed and then compared as signed integers


/*****************************************************************************
 *** AUXILIARY FUNCTIONS *****************************************************
 *****************************************************************************/

// File-local event logging helper
void logIt(int mid, int arg1, const char* arg2 = NULL)
{
    MessageLog logger(LoggingID(28));
    logging::Message::Args args;
    Message msg(mid);

    args.add(arg1);

    if (arg2 && *arg2)
        args.add(arg2);

    msg.format(args);
    logger.logErrorMessage(msg);
}

// Reverse the byte order
inline uint64_t order_swap(uint64_t x)
{
    uint64_t ret = (x >> 56) |
                   ((x << 40) & 0x00FF000000000000ULL) |
                   ((x << 24) & 0x0000FF0000000000ULL) |
                   ((x << 8)  & 0x000000FF00000000ULL) |
                   ((x >> 8)  & 0x00000000FF000000ULL) |
                   ((x >> 24) & 0x0000000000FF0000ULL) |
                   ((x >> 40) & 0x000000000000FF00ULL) |
                   (x << 56);
    return ret;
}

// Portable way to copy value
inline void copyValue(void* out, const void *in, size_t size)
{
    memcpy(out, in, size);  //// we are relying on little-endiannes here if actual *in has width >size
}

// char(8) values lose their null terminator
template <int COL_WIDTH>
inline string fixChar(int64_t intval)
{
    char chval[COL_WIDTH + 1];
    memcpy(chval, &intval, COL_WIDTH);
    chval[COL_WIDTH] = '\0';

    return string(chval);
}

//FIXME: what are we trying to accomplish here? It looks like we just want to count
// the chars in a string arg?
inline p_DataValue convertToPDataValue(const void* val, int COL_WIDTH)
{
    p_DataValue dv;
    string str;

    if (8 == COL_WIDTH)
        str = fixChar<8>(*reinterpret_cast<const int64_t*>(val));
    else
        str = reinterpret_cast<const char*>(val);

    dv.len = static_cast<int>(str.length());
    dv.data = reinterpret_cast<const uint8_t*>(val);
    return dv;
}


/*****************************************************************************
 *** NULL/EMPTY VALUES FOR EVERY COLUMN TYPE/WIDTH ***************************
 *****************************************************************************/

// Bit pattern representing EMPTY value for given column type/width
template<int COL_WIDTH>
uint64_t getEmptyValue(uint8_t type);

template<>
uint64_t getEmptyValue<8>(uint8_t type)
{
    switch (type)
    {
        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
            return joblist::DOUBLEEMPTYROW;

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
        case CalpontSystemCatalog::VARBINARY:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
            return joblist::CHAR8EMPTYROW;

        case CalpontSystemCatalog::UBIGINT:
            return joblist::UBIGINTEMPTYROW;

        default:
            return joblist::BIGINTEMPTYROW;
    }
}

template<>
uint64_t getEmptyValue<4>(uint8_t type)
{
    switch (type)
    {
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
            return joblist::FLOATEMPTYROW;

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return joblist::CHAR4EMPTYROW;

        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UMEDINT:
            return joblist::UINTEMPTYROW;

        default:
            return joblist::INTEMPTYROW;
    }
}

template<>
uint64_t getEmptyValue<2>(uint8_t type)
{
    switch (type)
    {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return joblist::CHAR2EMPTYROW;

        case CalpontSystemCatalog::USMALLINT:
            return joblist::USMALLINTEMPTYROW;

        default:
            return joblist::SMALLINTEMPTYROW;
    }
}

template<>
uint64_t getEmptyValue<1>(uint8_t type)
{
    switch (type)
    {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return joblist::CHAR1EMPTYROW;

        case CalpontSystemCatalog::UTINYINT:
            return joblist::UTINYINTEMPTYROW;

        default:
            return joblist::TINYINTEMPTYROW;
    }
}


// Bit pattern representing NULL value for given column type/width
template<int COL_WIDTH>
uint64_t getNullValue(uint8_t type);

template<>
uint64_t getNullValue<8>(uint8_t type)
{
    switch (type)
    {
        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
            return joblist::DOUBLENULL;

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
        case CalpontSystemCatalog::VARBINARY:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
            return joblist::CHAR8NULL;

        case CalpontSystemCatalog::UBIGINT:
            return joblist::UBIGINTNULL;

        default:
            return joblist::BIGINTNULL;
    }
}

template<>
uint64_t getNullValue<4>(uint8_t type)
{
    switch (type)
    {
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
            return joblist::FLOATNULL;

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
            return joblist::CHAR4NULL;

        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return joblist::DATENULL;

        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UMEDINT:
            return joblist::UINTNULL;

        default:
            return joblist::INTNULL;
    }
}

template<>
uint64_t getNullValue<2>(uint8_t type)
{
    switch (type)
    {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return joblist::CHAR2NULL;

        case CalpontSystemCatalog::USMALLINT:
            return joblist::USMALLINTNULL;

        default:
            return joblist::SMALLINTNULL;
    }
}

template<>
uint64_t getNullValue<1>(uint8_t type)
{
    switch (type)
    {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return joblist::CHAR1NULL;

        case CalpontSystemCatalog::UTINYINT:
            return joblist::UTINYINTNULL;

        default:
            return joblist::TINYINTNULL;
    }
}

inline bool colCompareStr(const ColRequestHeaderDataType &type,
                          uint8_t COP,
                          const utils::ConstString &val1,
                          const utils::ConstString &val2)
{
    int error = 0;
    bool rc = primitives::StringComparator(type).op(&error, COP, val1, val2);
    if (error)
    {
        logIt(34, COP, "colCompareStr");
        return false;  // throw an exception here?
    }
    return rc;
}

// Check whether val is NULL (or alternative NULL bit pattern for 64-bit string types)
template<ENUM_KIND KIND, typename T>
inline bool isNullValue(int64_t val, T NULL_VALUE)
{
    //@bug 339 might be a token here
    //TODO: what's up with the alternative NULL here?
    uint64_t ALT_NULL_VALUE = 0xFFFFFFFFFFFFFFFELL;

    constexpr int COL_WIDTH = sizeof(T);
    return (static_cast<T>(val) == NULL_VALUE) ||
           ((KIND_TEXT == KIND)  &&  (COL_WIDTH == 8)  &&  (val == ALT_NULL_VALUE));
}


/*****************************************************************************
 *** COMPARISON OPERATIONS FOR COLUMN VALUES *********************************
 *****************************************************************************/

template<class T>
inline bool colCompare_(const T& val1, const T& val2, uint8_t COP)
{
    switch (COP)
    {
        case COMPARE_NIL:
            return false;

        case COMPARE_LT:
            return val1 < val2;

        case COMPARE_EQ:
            return val1 == val2;

        case COMPARE_LE:
            return val1 <= val2;

        case COMPARE_GT:
            return val1 > val2;

        case COMPARE_NE:
            return val1 != val2;

        case COMPARE_GE:
            return val1 >= val2;

        default:
            logIt(34, COP, "colCompare_");
            return false;						// throw an exception here?
    }
}

template<class T>
inline bool colCompare_(const T& val1, const T& val2, uint8_t COP, uint8_t rf)
{
    switch (COP)
    {
        case COMPARE_NIL:
            return false;

        case COMPARE_LT:
            return val1 < val2 || (val1 == val2 && (rf & 0x01));

        case COMPARE_LE:
            return val1 < val2 || (val1 == val2 && rf ^ 0x80);

        case COMPARE_EQ:
            return val1 == val2 && rf == 0;

        case COMPARE_NE:
            return val1 != val2 || rf != 0;

        case COMPARE_GE:
            return val1 > val2 || (val1 == val2 && rf ^ 0x01);

        case COMPARE_GT:
            return val1 > val2 || (val1 == val2 && (rf & 0x80));

        default:
            logIt(34, COP, "colCompare_l");
            return false;						// throw an exception here?
    }


//@bug 1828  Like must be a string compare.
inline bool colStrCompare_(uint64_t val1, uint64_t val2, uint8_t COP, uint8_t rf)
{
    switch (COP)
    {
        case COMPARE_NIL:
            return false;

        case COMPARE_LT:
            return val1 < val2 || (val1 == val2 && rf != 0);

        case COMPARE_LE:
            return val1 <= val2;

        case COMPARE_EQ:
            return val1 == val2 && rf == 0;

        case COMPARE_NE:
            return val1 != val2 || rf != 0;

        case COMPARE_GE:
            return val1 > val2 || (val1 == val2 && rf == 0);

        case COMPARE_GT:
            return val1 > val2;

        case COMPARE_LIKE:
        case COMPARE_NLIKE:
        default:
            logIt(34, COP, "colStrCompare_");
            return false;						//TODO:  throw an exception here?
    }
}


// Compare two column values using given comparison operation,
// taking into account all rules about NULL values, string trimming and so on
template<ENUM_KIND KIND, int COL_WIDTH, bool IS_NULL = false>
inline bool colCompare(
    const ColRequestHeaderDataType &typeHolder,
    int64_t val1,
    int64_t val2,
    uint8_t COP,
    uint8_t rf,
    bool isVal2Null = false)
{
    if (COMPARE_NIL == COP) return false;

    //@bug 425 added IS_NULL condition
    else if (KIND_FLOAT == KIND  &&  !IS_NULL)
    {
        if (COL_WIDTH == 4)
        {
            float dVal1 = *((float*) &val1);
            float dVal2 = *((float*) &val2);
            return colCompare_(dVal1, dVal2, COP);
        }
        else
        {
            double dVal1 = *((double*) &val1);
            double dVal2 = *((double*) &val2);
            return colCompare_(dVal1, dVal2, COP);
        }
    }

    else if (KIND_TEXT == KIND  &&  !IS_NULL)
    {
        if (COP & COMPARE_LIKE) // LIKE and NOT LIKE
        {
            utils::ConstString subject = {reinterpret_cast<const char*>(&val1), width};
            utils::ConstString pattern = {reinterpret_cast<const char*>(&val2), width};
            return typeHolder.like(COP & COMPARE_NOT, subject.rtrimZero(),
                                                      pattern.rtrimZero());
        }

        if (!rf)
        {
            // A temporary hack for xxx_nopad_bin collations
            // TODO: MCOL-4534 Improve comparison performance in 8bit nopad_bin collations
            if ((typeHolder.getCharset().state & (MY_CS_BINSORT|MY_CS_NOPAD)) ==
                (MY_CS_BINSORT|MY_CS_NOPAD))
              return colCompare_(order_swap(val1), order_swap(val2), COP);
            utils::ConstString s1 = {reinterpret_cast<const char*>(&val1), width};
            utils::ConstString s2 = {reinterpret_cast<const char*>(&val2), width};
            return colCompareStr(typeHolder, COP, s1.rtrimZero(), s2.rtrimZero());
        }
        else
            return colStrCompare_(order_swap(val1), order_swap(val2), COP, rf);
    }

    else
    {
        if (IS_NULL == isVal2Null || (isVal2Null && COP == COMPARE_NE))
        {
            if (KIND_UNSIGNED == KIND)
            {
                uint64_t uval1 = val1, uval2 = val2;
                return colCompare_(uval1, uval2, COP, rf);
            }
            else
                return colCompare_(val1, val2, COP, rf);
        }
        else
            return false;
    }
}


/*****************************************************************************
 *** FILTER ENTIRE COLUMN ****************************************************
 *****************************************************************************/

// Provides 6 comparison operators for any datatype
template <int COP, typename T>
struct Comparator;

template <typename T>  struct Comparator<COMPARE_EQ, T>  {static bool compare(T val1, T val2)  {return val1 == val2;}};
template <typename T>  struct Comparator<COMPARE_NE, T>  {static bool compare(T val1, T val2)  {return val1 != val2;}};
template <typename T>  struct Comparator<COMPARE_GT, T>  {static bool compare(T val1, T val2)  {return val1 > val2;}};
template <typename T>  struct Comparator<COMPARE_LT, T>  {static bool compare(T val1, T val2)  {return val1 < val2;}};
template <typename T>  struct Comparator<COMPARE_GE, T>  {static bool compare(T val1, T val2)  {return val1 >= val2;}};
template <typename T>  struct Comparator<COMPARE_LE, T>  {static bool compare(T val1, T val2)  {return val1 <= val2;}};
template <typename T>  struct Comparator<COMPARE_NIL, T> {static bool compare(T val1, T val2)  {return false;}};

// Provides 3 combining operators for any flag type
template <int BOP, typename T>
struct Combiner;

template <typename T>  struct Combiner<BOP_AND, T>  {static void combine(T &flag, bool cmp)  {flag &= cmp;}};
template <typename T>  struct Combiner<BOP_OR,  T>  {static void combine(T &flag, bool cmp)  {flag |= cmp;}};
template <typename T>  struct Combiner<BOP_XOR, T>  {static void combine(T &flag, bool cmp)  {flag ^= cmp;}};


// Apply to dataArray[dataSize] column values the single filter element,
// consisting of comparison operator COP with a value to compare cmp_value,
// and combine the comparison result into the corresponding element of filterArray
// with combining operator BOP
template<int BOP, int COP, typename DATA_T, typename FILTER_ARRAY_T>
void applyFilterElement(
    size_t dataSize,
    const DATA_T* dataArray,
    DATA_T cmp_value,
    FILTER_ARRAY_T *filterArray)
{
    for (size_t i = 0; i < dataSize; ++i)
    {
        bool cmp = Comparator<COP, DATA_T>::compare(dataArray[i], cmp_value);
        Combiner<BOP,FILTER_ARRAY_T>::combine(filterArray[i], cmp);
    }
}

// Dispatch function by COP
template<int BOP, typename DATA_T, typename FILTER_ARRAY_T>
void applyFilterElement(
    int COP,
    size_t dataSize,
    const DATA_T* dataArray,
    DATA_T cmp_value,
    FILTER_ARRAY_T *filterArray)
{
    switch(COP)
    {
        case COMPARE_EQ:  applyFilterElement<BOP, COMPARE_EQ>(dataSize, dataArray, cmp_value, filterArray);  break;
        case COMPARE_NE:  applyFilterElement<BOP, COMPARE_NE>(dataSize, dataArray, cmp_value, filterArray);  break;
        case COMPARE_GT:  applyFilterElement<BOP, COMPARE_GT>(dataSize, dataArray, cmp_value, filterArray);  break;
        case COMPARE_LT:  applyFilterElement<BOP, COMPARE_LT>(dataSize, dataArray, cmp_value, filterArray);  break;
        case COMPARE_GE:  applyFilterElement<BOP, COMPARE_GE>(dataSize, dataArray, cmp_value, filterArray);  break;
        case COMPARE_LE:  applyFilterElement<BOP, COMPARE_LE>(dataSize, dataArray, cmp_value, filterArray);  break;
        case COMPARE_NIL: applyFilterElement<BOP, COMPARE_NIL>(dataSize,dataArray, cmp_value, filterArray);  break;
        default:          idbassert(0);
    }
}

template<int BOP, typename DATA_T, typename FILTER_ARRAY_T>
void applySetFilter(
    size_t dataSize,
    const DATA_T* dataArray,
    prestored_set_t* filterSet,     // Set of values for simple filters (any of values / none of them)
    FILTER_ARRAY_T *filterArray)
{
    for (size_t i = 0; i < dataSize; ++i)
    {
        bool found = (filterSet->find(dataArray[i]) != filterSet->end());
        filterArray[i] = (BOP_OR == BOP?  found : !found);
    }
}


/*****************************************************************************
 *** FILTER A COLUMN VALUE ***************************************************
 *****************************************************************************/

// Return true if curValue matches the filter represented by all those arrays
template<ENUM_KIND KIND, int COL_WIDTH, bool IS_NULL = false, typename T>
inline bool matchingColValue(
    const ColRequestHeaderDataType &typeHolder,
    // Value description
    int64_t curValue,               // The value (IS_NULL - is the value null?)
    // Filter description
    ColumnFilterMode columnFilterMode,
    prestored_set_t* filterSet,     // Set of values for simple filters (any of values / none of them)
    uint32_t filterCount,           // Number of filter elements, each described by one entry in the following arrays:
    uint8_t* filterCOPs,            //   comparison operation
    int64_t* filterValues,          //   value to compare to
    uint8_t* filterRFs,
    T NULL_VALUE)                   // Bit pattern representing NULL value for this column type/width
{
    /* In order to make filtering as fast as possible, we replaced the single generic algorithm
       with several algorithms, better tailored for more specific cases:
       empty filter, single comparison, and/or/xor comparison results, one/none of small/large set of values
    */
    switch (columnFilterMode)
    {
        // Empty filter is always true
        case ALWAYS_TRUE:
            return true;


        // Filter consisting of exactly one comparison operation
        case SINGLE_COMPARISON:
        {
            auto filterValue = filterValues[0];
            bool cmp = colCompare<KIND, COL_WIDTH, IS_NULL>(typeHolder, curValue, filterValue, filterCOPs[0],
                                                            filterRFs[0], isNullValue<KIND,T>(filterValue, NULL_VALUE));
            return cmp;
        }


        // Filter is true if ANY comparison is true (BOP_OR)
        case ANY_COMPARISON_TRUE:
        {
            for (int argIndex = 0; argIndex < filterCount; argIndex++)
            {
                auto filterValue = filterValues[argIndex];
                bool cmp = colCompare<KIND, COL_WIDTH, IS_NULL>(typeHolder, curValue, filterValue, filterCOPs[argIndex],
                                                                filterRFs[argIndex], isNullValue<KIND,T>(filterValue, NULL_VALUE));

                // Short-circuit the filter evaluation - true || ... == true
                if (cmp == true)
                    return true;
            }

            // We can get here only if all filters returned false
            return false;
        }


        // Filter is true only if ALL comparisons are true (BOP_AND)
        case ALL_COMPARISONS_TRUE:
        {
            for (int argIndex = 0; argIndex < filterCount; argIndex++)
            {
                auto filterValue = filterValues[argIndex];
                bool cmp = colCompare<KIND, COL_WIDTH, IS_NULL>(curValue, filterValue, filterCOPs[argIndex],
                                                                filterRFs[argIndex], filterRegexes[argIndex],
                                                                isNullValue<KIND,T>(filterValue, NULL_VALUE));

                // Short-circuit the filter evaluation - false && ... = false
                if (cmp == false)
                    return false;
            }

            // We can get here only if all filters returned true
            return true;
        }


        // XORing results of comparisons (BOP_XOR)
        case XOR_COMPARISONS:
        {
            bool result = false;

            for (int argIndex = 0; argIndex < filterCount; argIndex++)
            {
                auto filterValue = filterValues[argIndex];
                bool cmp = colCompare<KIND, COL_WIDTH, IS_NULL>(curValue, filterValue, filterCOPs[argIndex],
                                                                filterRFs[argIndex], filterRegexes[argIndex],
                                                                isNullValue<KIND,T>(filterValue, NULL_VALUE));
                result ^= cmp;
            }

            return result;
        }


        // ONE of the values in the small set represented by an array (BOP_OR + all COMPARE_EQ)
        case ONE_OF_VALUES_IN_ARRAY:
        {
            for (int argIndex = 0; argIndex < filterCount; argIndex++)
            {
                if (curValue == filterValues[argIndex])
                    return true;
            }

            return false;
        }


        // NONE of the values in the small set represented by an array (BOP_AND + all COMPARE_NE)
        case NONE_OF_VALUES_IN_ARRAY:
        {
            for (int argIndex = 0; argIndex < filterCount; argIndex++)
            {
                if (curValue == filterValues[argIndex])
                    return false;
            }

            return true;
        }


        // ONE of the values in the set is equal to the value checked (BOP_OR + all COMPARE_EQ)
        case ONE_OF_VALUES_IN_SET:
        {
            bool found = (filterSet->find(curValue) != filterSet->end());
            return found;
        }


        // NONE of the values in the set is equal to the value checked (BOP_AND + all COMPARE_NE)
        case NONE_OF_VALUES_IN_SET:
        {
            // bug 1920: ignore NULLs in the set and in the column data
            if (IS_NULL)
                return false;

            bool found = (filterSet->find(curValue) != filterSet->end());
            return !found;
        }


        default:
            idbassert(0);
            return true;
    }
}


/*****************************************************************************
 *** FIND COLUMN MIN/MAX *****************************************************
 *****************************************************************************/

// Set the minimum and maximum in the return header if we will be doing a block scan and
// we are dealing with a type that is comparable as a 64 bit integer.  Subsequent calls can then
// skip this block if the value being searched is outside of the Min/Max range.
bool isMinMaxValid(const NewColRequestHeader* in)
{
    if (in->NVALS != 0)
    {
        return false;
    }
    else
    {
        switch (in->DataType)
        {
            case CalpontSystemCatalog::CHAR:
                return (in->DataSize < 9);

            case CalpontSystemCatalog::VARCHAR:
            case CalpontSystemCatalog::BLOB:
            case CalpontSystemCatalog::TEXT:
                return (in->DataSize < 8);

            case CalpontSystemCatalog::TINYINT:
            case CalpontSystemCatalog::SMALLINT:
            case CalpontSystemCatalog::MEDINT:
            case CalpontSystemCatalog::INT:
            case CalpontSystemCatalog::DATE:
            case CalpontSystemCatalog::BIGINT:
            case CalpontSystemCatalog::DATETIME:
            case CalpontSystemCatalog::TIME:
            case CalpontSystemCatalog::TIMESTAMP:
            case CalpontSystemCatalog::UTINYINT:
            case CalpontSystemCatalog::USMALLINT:
            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::UBIGINT:
                return true;

            case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
                return (in->DataSize <= 8);

            default:
                return false;
        }
    }
}


// Find minimum and maximum among non-empty/null values in the array
template<bool SKIP_EMPTY_VALUES, typename T, typename VALTYPE>
void findMinMaxArray(
    size_t dataSize,
    const T* dataArray,
    VALTYPE* MinPtr,        // Place to store minimum column value
    VALTYPE* MaxPtr,        // Place to store maximum column value
    T EMPTY_VALUE,
    T NULL_VALUE)
{
    // Local vars to capture the min and max values
    VALTYPE Min = numeric_limits<VALTYPE>::max();
    VALTYPE Max = numeric_limits<VALTYPE>::min();

    for (size_t i = 0; i < dataSize; ++i)
    {
        auto curValue = dataArray[i];

        //TODO: optimize handling of NULL values by avoiding non-predictable jumps
        // If SKIP_EMPTY_VALUES==true, then empty cvalues were already dropped by readArray(), so we can skip this check
        if ((SKIP_EMPTY_VALUES || curValue != EMPTY_VALUE)  &&  curValue != NULL_VALUE)
        {
            VALTYPE value = static_cast<VALTYPE>(curValue);  // promote to int64 / uint64

            if (Min > value)
                Min = value;

            if (Max < value)
                Max = value;
        }
    }

    *MinPtr = Min;
    *MaxPtr = Max;
}


/*****************************************************************************
 *** READ COLUMN VALUES ******************************************************
 *****************************************************************************/

// Read one ColValue from the input data.
// Return true on success, false on EOF.
// Values are read from srcArray either in natural order or in the order defined by ridArray.
// Empty values are skipped, unless ridArray==0 && !(OutputType & OT_RID).
template<typename T, int COL_WIDTH>
inline bool nextColValue(
    int64_t* result,            // Place for the value returned
    bool* isEmpty,              // ... and flag whether it's EMPTY
    int* index,                 // Successive index either in srcArray (going from 0 to srcSize-1) or ridArray (0..ridSize-1)
    uint16_t* rid,              // Index in srcArray of the value returned
    const T* srcArray,          // Input array
    const unsigned srcSize,     // ... and its size
    const uint16_t* ridArray,   // Optional array of indexes into srcArray, that defines the read order
    const int ridSize,          // ... and its size
    const uint8_t OutputType,   // Used to decide whether to skip EMPTY values
    T EMPTY_VALUE)
{
    auto i = *index;    // local copy of *index to speed up loops
    T value;            // value to be written into *result, local for the same reason

    if (ridArray)
    {
        // Read next non-empty value in the order defined by ridArray
        for( ; ; i++)
        {
            if (UNLIKELY(i >= ridSize))
                return false;

            value = srcArray[ridArray[i]];

            if (value != EMPTY_VALUE)
                break;
        }

        *rid = ridArray[i];
        *isEmpty = false;
    }
    else if (OutputType & OT_RID)   //TODO: check correctness of this condition for SKIP_EMPTY_VALUES
    {
        // Read next non-empty value in the natural order
        for( ; ; i++)
        {
            if (UNLIKELY(i >= srcSize))
                return false;

            value = srcArray[i];

            if (value != EMPTY_VALUE)
                break;
        }

        *rid = i;
        *isEmpty = false;
    }
    else
    {
        // Read next value in the natural order
        if (UNLIKELY(i >= srcSize))
            return false;

        *rid = i;
        value = srcArray[i];
        *isEmpty = (value == EMPTY_VALUE);
    }

    //Bug 838, tinyint null problem
#if 0
    if (type == CalpontSystemCatalog::FLOAT)
    {
        // convert the float to a 64-bit type, return that w/o conversion
        double dTmp = (double) * ((float*) &srcArray[*rid]);
        *result = *((int64_t*) &dTmp);
    }
    else
        *result = srcArray[*rid];
#endif

    *index = i+1;
    *result = value;
    return true;
}


/* Scan srcArray[srcSize] either in the natural order
   or in the order provided by ridArray[ridSize] (when RID_ORDER==true),
   When SKIP_EMPTY_VALUES==true, skip values equal to EMPTY_VALUE.
   Save non-skipped values to dataArray[] and, when WRITE_RID==true, their indexes to dataRid[].
   Return number of values written to dataArray[]
*/
template <bool WRITE_RID, bool RID_ORDER, bool SKIP_EMPTY_VALUES, typename SRC_T, typename DST_T>
size_t readArray(
    const SRC_T* srcArray, size_t srcSize,
    DST_T* dataArray, RID_T* dataRid = NULL,
    const RID_T* ridArray = NULL, size_t ridSize = 0,
    const SRC_T EMPTY_VALUE = 0)
{
    // Depending on RID_ORDER, we will scan either ridSize elements of ridArray[] or srcSize elements of srcArray[]
    size_t inputSize = (RID_ORDER? ridSize : srcSize);
    auto out = dataArray;

    // Check that all employed arrays are non-NULL.
    // NOTE: unused arays may still be non-NULL in order to simplify calling code.
    idbassert(srcArray);
    idbassert(dataArray);
    if (RID_ORDER)
        idbassert(ridArray);
    if (WRITE_RID)
        idbassert(dataRid);
    if (SKIP_EMPTY_VALUES)
        idbassert(EMPTY_VALUE);

    for(size_t i=0; i < inputSize; i++)
    {
        size_t rid = (RID_ORDER? ridArray[i] : i);
        auto value = srcArray[rid];

        if (SKIP_EMPTY_VALUES? LIKELY(value != EMPTY_VALUE) : true)
        {
            *out++ = static_cast<DST_T>(value);

            if (WRITE_RID)
                *dataRid++ = rid;
        }
    }

    return out - dataArray;
}


/*****************************************************************************
 *** WRITE COLUMN VALUES *****************************************************
 *****************************************************************************/

// Append value to the output buffer with debug-time check for buffer overflow
template<typename T>
inline void checkedWriteValue(
    void* out,
    unsigned outSize,
    unsigned* outPos,
    const T* src,
    int errSubtype)
{
#ifdef PRIM_DEBUG

    if (sizeof(T) > outSize - *outPos)
    {
        logIt(35, errSubtype);
        throw logic_error("PrimitiveProcessor::checkedWriteValue(): output buffer is too small");
    }

#endif

    uint8_t* out8 = reinterpret_cast<uint8_t*>(out);
    memcpy(out8 + *outPos, src, sizeof(T));
    *outPos += sizeof(T);
}


// Write the value index in srcArray and/or the value itself, depending on bits in OutputType,
// into the output buffer and update the output pointer.
template<typename T>
inline void writeColValue(
    uint8_t OutputType,
    NewColResultHeader* out,
    unsigned outSize,
    unsigned* written,
    uint16_t rid,
    const T* srcArray)
{
    if (OutputType & OT_RID)
    {
        checkedWriteValue(out, outSize, written, &rid, 1);
        out->RidFlags |= (1 << (rid >> 10)); // set the (row/1024)'th bit
    }

    if (OutputType & (OT_TOKEN | OT_DATAVALUE))
    {
        checkedWriteValue(out, outSize, written, &srcArray[rid], 2);
    }

    out->NVALS++;   //TODO: Can be computed at the end from *written value
}


template <bool WRITE_RID, bool WRITE_DATA, bool IS_NULL_VALUE_MATCHES, typename FILTER_ARRAY_T, typename RID_T, typename T>
void writeArray(
    size_t dataSize,
    const T* dataArray,
    const RID_T* dataRid,
    const FILTER_ARRAY_T *filterArray,
    uint8_t* outbuf,
    unsigned* written,
    uint16_t* NVALS,
    uint8_t* RidFlagsPtr,
    T NULL_VALUE)
{
    uint8_t* out = outbuf;
    uint8_t RidFlags = *RidFlagsPtr;

    for (size_t i = 0; i < dataSize; ++i)
    {
        //TODO: optimize handling of NULL values and flags by avoiding non-predictable jumps
        if (dataArray[i]==NULL_VALUE? IS_NULL_VALUE_MATCHES : filterArray[i])
        {
            if (WRITE_RID)
            {
                copyValue(out, &dataRid[i], sizeof(RID_T));
                out += sizeof(RID_T);

                RidFlags |= (1 << (dataRid[i] >> 10)); // set the (row/1024)'th bit
            }

            if (WRITE_DATA)
            {
                copyValue(out, &dataArray[i], sizeof(T));
                out += sizeof(T);
            }
        }
    }

    // Update number of written values, number of written bytes and out->RidFlags
    int size1 = (WRITE_RID? sizeof(RID_T) : 0) + (WRITE_DATA? sizeof(T) : 0);
    *NVALS += (out - outbuf) / size1;
    *written += out - outbuf;
    *RidFlagsPtr = RidFlags;
}


/*****************************************************************************
 *** COMPILE A COLUMN FILTER *************************************************
 *****************************************************************************/

// Compile column filter from BLOB into structure optimized for fast filtering.
// Return the compiled filter.
template<typename T>                // C++ integer type providing storage for colType
boost::shared_ptr<ParsedColumnFilter> parseColumnFilter_T(
    const uint8_t* filterString,    // Filter represented as BLOB
    uint32_t colType,               // Column datatype as ColDataType
    uint32_t filterCount,           // Number of filter elements contained in filterString
    uint32_t BOP)                   // Operation (and/or/xor/none) that combines all filter elements
{
    const uint32_t COL_WIDTH = sizeof(T);  // Sizeof of the column to be filtered

    boost::shared_ptr<ParsedColumnFilter> ret;  // Place for building the value to return
    if (filterCount == 0)
        return ret;

    // Allocate the compiled filter structure with space for filterCount filters.
    // No need to init arrays since they will be filled on the fly.
    ret.reset(new ParsedColumnFilter());
    if (datatypes::isWideDecimalType((CalpontSystemCatalog::ColDataType)colType, colWidth))
        ret->prestored_argVals128.reset(new int128_t[filterCount]);
    else
        ret->prestored_argVals.reset(new int64_t[filterCount]);

    ret->prestored_cops.reset(new uint8_t[filterCount]);
    ret->prestored_rfs.reset(new uint8_t[filterCount]);

    // Choose initial filter mode based on operation and number of filter elements
    if (filterCount == 1)
        ret->columnFilterMode = SINGLE_COMPARISON;
    else if (BOP == BOP_OR)
        ret->columnFilterMode = ANY_COMPARISON_TRUE;
    else if (BOP == BOP_AND)
        ret->columnFilterMode = ALL_COMPARISONS_TRUE;
    else if (BOP == BOP_XOR)
        ret->columnFilterMode = XOR_COMPARISONS;
    else
        idbassert(0);   // BOP_NONE is compatible only with filterCount <= 1


    // Parse the filter predicates and insert them into argVals and cops
    for (uint32_t argIndex = 0; argIndex < filterCount; argIndex++)
    {
        // Size of single filter element in filterString BLOB
        const uint32_t filterSize = sizeof(uint8_t) + sizeof(uint8_t) + COL_WIDTH;

        // Pointer to ColArgs structure representing argIndex'th element in the BLOB
        auto args = reinterpret_cast<const ColArgs*>(filterString + (argIndex * filterSize));

        ret->prestored_cops[argIndex] = args->COP;
        ret->prestored_rfs[argIndex] = args->rf;

#if 0
        if (colType == CalpontSystemCatalog::FLOAT)
        {
            double dTmp;

            dTmp = (double) * ((const float*) args->val);
            ret->prestored_argVals[argIndex] = *((int64_t*) &dTmp);
        }
        else
#else
        ret->prestored_argVals[argIndex] = *reinterpret_cast<const T*>(args->val);
#endif

//      cout << "inserted* " << hex << ret->prestored_argVals[argIndex] << dec <<
//        " COP = " << (int) ret->prestored_cops[argIndex] << endl;
    }


    /*  Decide which structure to use.  I think the only cases where we can use the set
        are when NOPS > 1, BOP is OR, and every COP is ==,
        and when NOPS > 1, BOP is AND, and every COP is !=.
        If there were no predicates that violate the condition for using a set,
        insert argVals into a set.
    */
    if (filterCount > 1)
    {
        // Check that all COPs are of right kind that depends on BOP
        for (uint32_t argIndex = 0; argIndex < filterCount; argIndex++)
        {
            auto cop = ret->prestored_cops[argIndex];

            if (! ((BOP == BOP_OR  && cop == COMPARE_EQ) ||
                   (BOP == BOP_AND && cop == COMPARE_NE)))
            {
                goto skipConversion;
            }
        }


        // Now we found that conversion is possible. Let's choose between array-based search
        // and set-based search depending on the set size.
        //TODO: Tailor the threshold based on the actual search algorithms used and COL_WIDTH/SIMD_WIDTH

        if (filterCount <= 8)
        {
            // Assign filter mode of array-based filtering
            if (BOP == BOP_OR)
                ret->columnFilterMode = ONE_OF_VALUES_IN_ARRAY;
            else
                ret->columnFilterMode = NONE_OF_VALUES_IN_ARRAY;
        }
        else
        {
            // Assign filter mode of set-based filtering
            if (BOP == BOP_OR)
                ret->columnFilterMode = ONE_OF_VALUES_IN_SET;
            else
                ret->columnFilterMode = NONE_OF_VALUES_IN_SET;

            // @bug 2584, use COMPARE_NIL for "= null" to allow "is null" in OR expression
            ret->prestored_set.reset(new prestored_set_t());
            for (uint32_t argIndex = 0; argIndex < filterCount; argIndex++)
                if (ret->prestored_rfs[argIndex] == 0)
                    ret->prestored_set->insert(ret->prestored_argVals[argIndex]);
        }

        skipConversion:;
    }

    return ret;
}


/*****************************************************************************
 *** RUN DATA THROUGH A COLUMN FILTER ****************************************
 *****************************************************************************/

/* "Vertical" processing of the column filter:
   1. load all data into temporary vector
   2. process one filter element over entire vector before going to a next one
   3. write records, that succesfully passed through the filter, to outbuf
*/
template<typename T, ENUM_KIND KIND, typename VALTYPE>
void processArray(
    // Source data
    const T* srcArray,
    size_t srcSize,
    uint16_t* ridArray,
    size_t ridSize,                 // Number of values in ridArray
    // Filter description
    int BOP,
    prestored_set_t* filterSet,     // Set of values for simple filters (any of values / none of them)
    uint32_t filterCount,           // Number of filter elements, each described by one entry in the following arrays:
    uint8_t* filterCOPs,            //   comparison operation
    int64_t* filterValues,          //   value to compare to
    // Output buffer/stats
    uint8_t* outbuf,                // Pointer to the place for output data
    unsigned* written,              // Number of written bytes, that we need to update
    uint16_t* NVALS,                // Number of written values, that we need to update
    uint8_t* RidFlagsPtr,           // Pointer to out->RidFlags
    // Processing parameters
    bool WRITE_RID,
    bool WRITE_DATA,
    bool SKIP_EMPTY_VALUES,
    T EMPTY_VALUE,
    bool IS_NULL_VALUE_MATCHES,
    T NULL_VALUE,
    // Min/Max search
    bool ValidMinMax,
    VALTYPE* MinPtr,
    VALTYPE* MaxPtr)
{
    // Alloc temporary arrays
    size_t inputSize = (ridArray? ridSize : srcSize);

    // Temporary array with data to filter
    std::vector<T> dataVec(inputSize);
    auto dataArray = dataVec.data();

    // Temporary array with RIDs of corresponding dataArray elements
    std::vector<RID_T> dataRidVec(WRITE_RID? inputSize : 0);
    auto dataRid = dataRidVec.data();


    // Copy input data into temporary array, opt. storing RIDs, opt. skipping EMPTYs
    size_t dataSize;  // number of values copied into dataArray
    if (ridArray != NULL)
    {
        SKIP_EMPTY_VALUES = true;  // let findMinMaxArray() know that empty values will be skipped

        dataSize = WRITE_RID? readArray<true, true,true>(srcArray, srcSize, dataArray, dataRid, ridArray, ridSize, EMPTY_VALUE)
                            : readArray<false,true,true>(srcArray, srcSize, dataArray, dataRid, ridArray, ridSize, EMPTY_VALUE);
    }
    else if (SKIP_EMPTY_VALUES)
    {
        dataSize = WRITE_RID? readArray<true, false,true>(srcArray, srcSize, dataArray, dataRid, ridArray, ridSize, EMPTY_VALUE)
                            : readArray<false,false,true>(srcArray, srcSize, dataArray, dataRid, ridArray, ridSize, EMPTY_VALUE);
    }
    else
    {
        dataSize = WRITE_RID? readArray<true, false,false>(srcArray, srcSize, dataArray, dataRid, ridArray, ridSize, EMPTY_VALUE)
                            : readArray<false,false,false>(srcArray, srcSize, dataArray, dataRid, ridArray, ridSize, EMPTY_VALUE);
    }

    // If required, find Min/Max values of the data
    if (ValidMinMax)
    {
        SKIP_EMPTY_VALUES? findMinMaxArray<true> (dataSize, dataArray, MinPtr, MaxPtr, EMPTY_VALUE, NULL_VALUE)
                         : findMinMaxArray<false>(dataSize, dataArray, MinPtr, MaxPtr, EMPTY_VALUE, NULL_VALUE);
    }


    // Choose initial filterArray[i] value depending on the operation
    bool initValue = false;
    if      (filterCount == 0) {initValue = true;}
    else if (BOP_NONE == BOP)  {initValue = false;  BOP = BOP_OR;}
    else if (BOP_OR   == BOP)  {initValue = false;}
    else if (BOP_XOR  == BOP)  {initValue = false;}
    else if (BOP_AND  == BOP)  {initValue = true;}

    // Temporary array accumulating results of filtering for each record
    std::vector<uint8_t> filterVec(dataSize, initValue);
    auto filterArray = filterVec.data();

    // Real type of column data, may be floating-point (used only for comparisons in the filtering)
    using FLOAT_T = typename std::conditional<sizeof(T) == 8, double, float>::type;
    using DATA_T  = typename std::conditional<KIND_FLOAT == KIND, FLOAT_T, T>::type;
    auto realDataArray = reinterpret_cast<DATA_T*>(dataArray);


    // Evaluate column filter on elements of dataArray and store results into filterArray
    if (filterSet != NULL  &&  BOP == BOP_OR)
    {
        applySetFilter<BOP_OR>(dataSize, dataArray, filterSet, filterArray);
    }
    else if (filterSet != NULL  &&  BOP == BOP_AND)
    {
        applySetFilter<BOP_AND>(dataSize, dataArray, filterSet, filterArray);
    }
    else
    {
        for (int i = 0; i < filterCount; ++i)
        {
            DATA_T cmp_value;   // value for comparison, may be floating-point
            copyValue(&cmp_value, &filterValues[i], sizeof(cmp_value));

            switch(BOP)
            {
                case BOP_AND:  applyFilterElement<BOP_AND>(filterCOPs[i], dataSize, realDataArray, cmp_value, filterArray);  break;
                case BOP_OR:   applyFilterElement<BOP_OR> (filterCOPs[i], dataSize, realDataArray, cmp_value, filterArray);  break;
                case BOP_XOR:  applyFilterElement<BOP_XOR>(filterCOPs[i], dataSize, realDataArray, cmp_value, filterArray);  break;
                default:       idbassert(0);
            }
        }
    }


    // Copy filtered data and/or their RIDs into output buffer
    if (WRITE_RID && WRITE_DATA)
    {
        IS_NULL_VALUE_MATCHES? writeArray<true,true,true> (dataSize, dataArray, dataRid, filterArray, outbuf, written, NVALS, RidFlagsPtr, NULL_VALUE)
                             : writeArray<true,true,false>(dataSize, dataArray, dataRid, filterArray, outbuf, written, NVALS, RidFlagsPtr, NULL_VALUE);
    }
    else if (WRITE_RID)
    {
        IS_NULL_VALUE_MATCHES? writeArray<true,false,true> (dataSize, dataArray, dataRid, filterArray, outbuf, written, NVALS, RidFlagsPtr, NULL_VALUE)
                             : writeArray<true,false,false>(dataSize, dataArray, dataRid, filterArray, outbuf, written, NVALS, RidFlagsPtr, NULL_VALUE);
    }
    else
    {
        IS_NULL_VALUE_MATCHES? writeArray<false,true,true> (dataSize, dataArray, dataRid, filterArray, outbuf, written, NVALS, RidFlagsPtr, NULL_VALUE)
                             : writeArray<false,true,false>(dataSize, dataArray, dataRid, filterArray, outbuf, written, NVALS, RidFlagsPtr, NULL_VALUE);
    }
}


// Copy data matching parsedColumnFilter from input to output.
// Input is srcArray[srcSize], optionally accessed in the order defined by ridArray[ridSize].
// Output is BLOB out[outSize], written starting at offset *written, which is updated afterward.
template<typename T, ENUM_KIND KIND>
void filterColumnData(
    NewColRequestHeader* in,
    NewColResultHeader* out,
    unsigned outSize,
    unsigned* written,
    uint16_t* ridArray,
    int ridSize,                // Number of values in ridArray
    int* srcArray16,
    unsigned srcSize,
    boost::shared_ptr<ParsedColumnFilter> parsedColumnFilter)
{
    constexpr int COL_WIDTH = sizeof(T);
    const T* srcArray = reinterpret_cast<const T*>(srcArray16);

    // Cache some structure fields in local vars
    auto DataType = (CalpontSystemCatalog::ColDataType) in->DataType;  // Column datatype
    uint32_t filterCount = in->NOPS;        // Number of elements in the filter
    uint8_t  OutputType  = in->OutputType;

    // If no pre-parsed column filter is set, parse the filter in the message
    if (parsedColumnFilter.get() == NULL  &&  filterCount > 0)
        parsedColumnFilter = parseColumnFilter_T<T>((uint8_t*)in + sizeof(NewColRequestHeader), in->DataType, in->NOPS, in->BOP);

    // Cache parsedColumnFilter fields in local vars
    auto columnFilterMode = (filterCount==0? ALWAYS_TRUE : parsedColumnFilter->columnFilterMode);
    auto filterValues  = (filterCount==0? NULL : parsedColumnFilter->prestored_argVals.get());
    auto filterCOPs    = (filterCount==0? NULL : parsedColumnFilter->prestored_cops.get());
    auto filterRFs     = (filterCount==0? NULL : parsedColumnFilter->prestored_rfs.get());
    auto filterSet     = (filterCount==0? NULL : parsedColumnFilter->prestored_set.get());

    // Bit patterns in srcArray[i] representing EMPTY and NULL values
    T EMPTY_VALUE = static_cast<T>(getEmptyValue<COL_WIDTH>(DataType));
    T NULL_VALUE  = static_cast<T>(getNullValue <COL_WIDTH>(DataType));

    // Precompute filter results for EMPTY and NULL values
    bool isEmptyValueMatches = matchingColValue<KIND, COL_WIDTH, false>(in->colType, EMPTY_VALUE, columnFilterMode, filterSet, filterCount,
                                    filterCOPs, filterValues, filterRFs, NULL_VALUE);

    bool isNullValueMatches = matchingColValue<KIND, COL_WIDTH, true>(in->colType, NULL_VALUE, columnFilterMode, filterSet, filterCount,
                                    filterCOPs, filterValues, filterRFs, NULL_VALUE);

    // Boolean indicating whether to capture the min and max values
    bool ValidMinMax = isMinMaxValid(in);
    // Real type of values captured in Min/Max
    using VALTYPE = typename std::conditional<KIND_UNSIGNED == KIND, uint64_t, int64_t>::type;
    // Local vars to capture the min and max values
    VALTYPE Min = numeric_limits<VALTYPE>::max();
    VALTYPE Max = numeric_limits<VALTYPE>::min();


    // If possible, use faster "vertical" filtering approach
    if (KIND != KIND_TEXT)
    {
        bool canUseFastFiltering = true;
        for (int i = 0; i < filterCount; ++i)
            if (filterRFs[i] != 0)
                canUseFastFiltering = false;

        if (canUseFastFiltering)
        {
            processArray<T, KIND, VALTYPE>(srcArray, srcSize, ridArray, ridSize,
                         in->BOP, filterSet, filterCount, filterCOPs, filterValues,
                         reinterpret_cast<uint8_t*>(out) + *written,
                         written, & out->NVALS, & out->RidFlags,
                         (OutputType & OT_RID) != 0,
                         (OutputType & (OT_TOKEN | OT_DATAVALUE)) != 0,
                         (OutputType & OT_RID) != 0,  //TODO: check correctness of this condition for SKIP_EMPTY_VALUES
                         EMPTY_VALUE,
                         isNullValueMatches, NULL_VALUE,
                         ValidMinMax, &Min, &Max);
            return;
        }
    }


    // Loop-local variables
    int64_t curValue = 0;
    uint16_t rid = 0;
    bool isEmpty = false;
    idb_regex_t placeholderRegex;
    placeholderRegex.used = false;

    // Loop over the column values, storing those matching the filter, and updating the min..max range
    for (int i = 0;
         nextColValue<T, COL_WIDTH>(&curValue, &isEmpty,
                                    &i, &rid,
                                    srcArray, srcSize, ridArray, ridSize,
                                    OutputType, EMPTY_VALUE); )
    {
        if (isEmpty)
        {
            // If EMPTY values match the filter, write curValue to the output buffer
            if (isEmptyValueMatches)
                writeColValue<T>(OutputType, out, outSize, written, rid, srcArray);
        }
        else if (isNullValue<KIND,T>(curValue, NULL_VALUE))
        {
            // If NULL values match the filter, write curValue to the output buffer
            if (isNullValueMatches)
                writeColValue<T>(OutputType, out, outSize, written, rid, srcArray);
        }
        else
        {
            // If curValue matches the filter, write it to the output buffer
            if (matchingColValue<KIND, COL_WIDTH, false>(in->colType, curValue, columnFilterMode, filterSet, filterCount,
                                filterCOPs, filterValues, filterRFs, NULL_VALUE))
            {
                writeColValue<T>(OutputType, out, outSize, written, rid, srcArray);
            }

            // Update Min and Max if necessary.  EMPTY/NULL values are processed in other branches.
            if (ValidMinMax)
            {
                if ((KIND_TEXT == KIND) && (COL_WIDTH > 1))
                {
                    // When computing Min/Max for string fields, we compare them trimWhitespace()'d
                    if (colCompare<KIND, COL_WIDTH>(in->colType, Min, curValue, COMPARE_GT, false))
                        Min = curValue;

                    if (colCompare<KIND, COL_WIDTH>(in->colType, Max, curValue, COMPARE_LT, false))
                        Max = curValue;
                }
                else
                {
                    VALTYPE value = static_cast<VALTYPE>(curValue);

                    if (Min > value)
                        Min = value;

                    if (Max < value)
                        Min = value;
                }
            }
        }
    }


    // Write captured Min/Max values to *out
    out->ValidMinMax = ValidMinMax;
    if (ValidMinMax)
    {
        out->Min = Min;
        out->Max = Max;
    }
}

} //namespace anon

namespace primitives
{

void PrimitiveProcessor::p_Col(NewColRequestHeader* in, NewColResultHeader* out,
                               unsigned outSize, unsigned* written)
{
    void *outp = static_cast<void*>(out);
    memcpy(outp, in, sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
    out->NVALS = 0;
    out->LBID = in->LBID;
    out->ism.Command = COL_RESULTS;
    out->OutputType = in->OutputType;
    out->RidFlags = 0;
    *written = sizeof(NewColResultHeader);
    unsigned itemsPerBlock = logicalBlockMode ? BLOCK_SIZE
                                              : BLOCK_SIZE / in->DataSize;

    //...Initialize I/O counts;
    out->CacheIO    = 0;
    out->PhysicalIO = 0;

#if 0

    // short-circuit the actual block scan for testing
    if (out->LBID >= 802816)
    {
        out->ValidMinMax = false;
        out->Min = 0;
        out->Max = 0;
        return;
    }

#endif

    auto markEvent = [&] (char eventChar)
    {
        if (fStatsPtr)
#ifdef _MSC_VER
            fStatsPtr->markEvent(in->LBID, GetCurrentThreadId(), in->hdr.SessionID, eventChar);
#else
            fStatsPtr->markEvent(in->LBID, pthread_self(), in->hdr.SessionID, eventChar);
#endif
    };

    markEvent('B');

    // Prepare ridArray (the row index array)
    uint16_t* ridArray = 0;
    int ridSize = in->NVALS;                // Number of values in ridArray
    if (ridSize > 0)
    {
        int filterSize = sizeof(uint8_t) + sizeof(uint8_t) + in->DataSize;
        ridArray = reinterpret_cast<uint16_t*>((uint8_t*)in + sizeof(NewColRequestHeader) + (in->NOPS * filterSize));

        if (1 == in->sort )
        {
            std::sort(ridArray, ridArray + ridSize);
            markEvent('O');
        }
    }

    auto DataType = (CalpontSystemCatalog::ColDataType) in->DataType;

    // Dispatch filtering by the column datatype/width in order to make it faster
    if (DataType == CalpontSystemCatalog::FLOAT)
    {
        idbassert(in->DataSize == 4);
        filterColumnData<int32_t, KIND_FLOAT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);
    }
    else if (DataType == CalpontSystemCatalog::DOUBLE)
    {
        idbassert(in->DataSize == 8);
        filterColumnData<int64_t, KIND_FLOAT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);
    }
    else if (DataType == CalpontSystemCatalog::CHAR ||
             DataType == CalpontSystemCatalog::VARCHAR ||
             DataType == CalpontSystemCatalog::TEXT)
    {
        switch (in->DataSize)
        {
            case 1:  filterColumnData< int8_t, KIND_TEXT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 2:  filterColumnData<int16_t, KIND_TEXT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 4:  filterColumnData<int32_t, KIND_TEXT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 8:  filterColumnData<int64_t, KIND_TEXT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            default: idbassert(0);
        }
    }
    else if (isUnsigned(DataType))
    {
        switch (in->DataSize)
        {
            case 1:  filterColumnData< uint8_t, KIND_UNSIGNED>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 2:  filterColumnData<uint16_t, KIND_UNSIGNED>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 4:  filterColumnData<uint32_t, KIND_UNSIGNED>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 8:  filterColumnData<uint64_t, KIND_UNSIGNED>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            default: idbassert(0);
        }
    }
    else
    {
        switch (in->DataSize)
        {
            case 1:  filterColumnData< int8_t, KIND_DEFAULT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 2:  filterColumnData<int16_t, KIND_DEFAULT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 4:  filterColumnData<int32_t, KIND_DEFAULT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 8:  filterColumnData<int64_t, KIND_DEFAULT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;
            case 16: filterColumnData<int128_t, KIND_DEFAULT>(in, out, outSize, written, ridArray, ridSize, block, itemsPerBlock, parsedColumnFilter);  break;

            default: idbassert(0);
        }
    }

    markEvent('C');
}


// Compile column filter from BLOB into structure optimized for fast filtering.
// Returns the compiled filter.
boost::shared_ptr<ParsedColumnFilter> parseColumnFilter(
    const uint8_t* filterString,    // Filter represented as BLOB
    uint32_t colWidth,              // Sizeof of the column to be filtered
    uint32_t colType,               // Column datatype as ColDataType
    uint32_t filterCount,           // Number of filter elements contained in filterString
    uint32_t BOP)                   // Operation (and/or/xor/none) that combines all filter elements
{
    // Dispatch by the column type to make it faster
    if (isUnsigned((CalpontSystemCatalog::ColDataType)colType))
    {
        switch (colWidth)
        {
            case 1:  return parseColumnFilter_T< uint8_t>(filterString, colType, filterCount, BOP);
            case 2:  return parseColumnFilter_T<uint16_t>(filterString, colType, filterCount, BOP);
            case 4:  return parseColumnFilter_T<uint32_t>(filterString, colType, filterCount, BOP);
            case 8:  return parseColumnFilter_T<uint64_t>(filterString, colType, filterCount, BOP);
        }
    }
    else
    {
        switch (colWidth)
        {
            case 1:  return parseColumnFilter_T< int8_t>(filterString, colType, filterCount, BOP);
            case 2:  return parseColumnFilter_T<int16_t>(filterString, colType, filterCount, BOP);
            case 4:  return parseColumnFilter_T<int32_t>(filterString, colType, filterCount, BOP);
            case 8:  return parseColumnFilter_T<int64_t>(filterString, colType, filterCount, BOP);
        }
    }

    logIt(36, colType*100 + colWidth, "parseColumnFilter");
    return NULL;   //FIXME: support for wider columns
}


// ######################################
namespace
{

template <class T>
inline int  compareBlock(  const void* a, const void* b )
{
    return ( (*(T*)a) - (*(T*)b) );
}

template<class T>
inline bool colCompare_(const T& val1, const T& val2, uint8_t COP)
{
    switch (COP)
    {
        case COMPARE_NIL:
            return false;

        case COMPARE_LT:
            return val1 < val2;

        case COMPARE_EQ:
            return val1 == val2;

        case COMPARE_LE:
            return val1 <= val2;

        case COMPARE_GT:
            return val1 > val2;

        case COMPARE_NE:
            return val1 != val2;

        case COMPARE_GE:
            return val1 >= val2;

        default:
            logIt(34, COP, "colCompare");
            return false;						// throw an exception here?
    }
}



template<class T>
inline bool colCompare_(const T& val1, const T& val2, uint8_t COP, uint8_t rf)
{
    switch (COP)
    {
        case COMPARE_NIL:
            return false;

        case COMPARE_LT:
            return val1 < val2 || (val1 == val2 && (rf & 0x01));

        case COMPARE_LE:
            return val1 < val2 || (val1 == val2 && rf ^ 0x80);

        case COMPARE_EQ:
            return val1 == val2 && rf == 0;

        case COMPARE_NE:
            return val1 != val2 || rf != 0;

        case COMPARE_GE:
            return val1 > val2 || (val1 == val2 && rf ^ 0x01);

        case COMPARE_GT:
            return val1 > val2 || (val1 == val2 && (rf & 0x80));

        default:
            logIt(34, COP, "colCompare_l");
            return false;						// throw an exception here?
    }
}


//@bug 1828  Like must be a string compare.
inline bool colStrCompare_(uint64_t val1, uint64_t val2, uint8_t COP, uint8_t rf)
{
    switch (COP)
    {
        case COMPARE_NIL:
            return false;

        case COMPARE_LT:
            return val1 < val2 || (val1 == val2 && rf != 0);

        case COMPARE_LE:
            return val1 <= val2;

        case COMPARE_EQ:
            return val1 == val2 && rf == 0;

        case COMPARE_NE:
            return val1 != val2 || rf != 0;

        case COMPARE_GE:
            return val1 > val2 || (val1 == val2 && rf == 0);

        case COMPARE_GT:
            return val1 > val2;

        case COMPARE_LIKE:
        case COMPARE_NLIKE:
        default:
            logIt(34, COP, "colCompare_l");
            return false;						// throw an exception here?
    }
}


template<int>
inline bool isEmptyVal(uint8_t type, const uint8_t* val8);

template<>
inline bool isEmptyVal<32>(uint8_t type, const uint8_t* ival) // For BINARY
{
    std::cout << __func__ << " WARNING!!! Not implemented for 32 byte data types." << std::endl;
    return false;
}

template<>
inline bool isEmptyVal<16>(uint8_t type, const uint8_t* ival) // For BINARY
{
    const int128_t* val = reinterpret_cast<const int128_t*>(ival);
    // Wide-DECIMAL supplies a universal NULL/EMPTY magics for all 16 byte
    // data types.
    return *val == datatypes::Decimal128Empty;
}

template<>
inline bool isEmptyVal<8>(uint8_t type, const uint8_t* ival)
{
    const uint64_t* val = reinterpret_cast<const uint64_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
            return (joblist::DOUBLEEMPTYROW == *val);

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
        case CalpontSystemCatalog::VARBINARY:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
            return (*val == joblist::CHAR8EMPTYROW);

        case CalpontSystemCatalog::UBIGINT:
            return (joblist::UBIGINTEMPTYROW == *val);

        default:
            break;
    }

    return (joblist::BIGINTEMPTYROW == *val);
}

template<>
inline bool isEmptyVal<4>(uint8_t type, const uint8_t* ival)
{
    const uint32_t* val = reinterpret_cast<const uint32_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
            return (joblist::FLOATEMPTYROW == *val);

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return (joblist::CHAR4EMPTYROW == *val);

        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UMEDINT:
            return (joblist::UINTEMPTYROW == *val);

        default:
            break;
    }

    return (joblist::INTEMPTYROW == *val);
}

template<>
inline bool isEmptyVal<2>(uint8_t type, const uint8_t* ival)
{
    const uint16_t* val = reinterpret_cast<const uint16_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return (joblist::CHAR2EMPTYROW == *val);

        case CalpontSystemCatalog::USMALLINT:
            return (joblist::USMALLINTEMPTYROW == *val);

        default:
            break;
    }

    return (joblist::SMALLINTEMPTYROW == *val);
}

template<>
inline bool isEmptyVal<1>(uint8_t type, const uint8_t* ival)
{
    const uint8_t* val = reinterpret_cast<const uint8_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return (*val == joblist::CHAR1EMPTYROW);

        case CalpontSystemCatalog::UTINYINT:
            return (*val == joblist::UTINYINTEMPTYROW);

        default:
            break;
    }

    return (*val == joblist::TINYINTEMPTYROW);
}

template<int>
inline bool isNullVal(uint8_t type, const uint8_t* val8);

template<>
inline bool isNullVal<16>(uint8_t type, const uint8_t* ival)
{
    const int128_t* val = reinterpret_cast<const int128_t*>(ival);
    // Wide-DECIMAL supplies a universal NULL/EMPTY magics for all 16 byte
    // data types.
    return *val == datatypes::Decimal128Null;
}

template<>
inline bool isNullVal<32>(uint8_t type, const uint8_t* ival)
{

    std::cout << __func__ << " WARNING!!! Not implemented for 32 byte data types."
<< std::endl;
    return false;
}

template<>
inline bool isNullVal<8>(uint8_t type, const uint8_t* ival)
{
    const uint64_t* val = reinterpret_cast<const uint64_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::DOUBLE:
        case CalpontSystemCatalog::UDOUBLE:
            return (joblist::DOUBLENULL == *val);

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
        case CalpontSystemCatalog::VARBINARY:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
            //@bug 339 might be a token here
            //TODO: what's up with the second const here?
            return (*val == joblist::CHAR8NULL || 0xFFFFFFFFFFFFFFFELL == *val);

        case CalpontSystemCatalog::UBIGINT:
            return (joblist::UBIGINTNULL == *val);

        default:
            break;
    }

    return (joblist::BIGINTNULL == *val);
}

template<>
inline bool isNullVal<4>(uint8_t type, const uint8_t* ival)
{
    const uint32_t* val = reinterpret_cast<const uint32_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::FLOAT:
        case CalpontSystemCatalog::UFLOAT:
            return (joblist::FLOATNULL == *val);

        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
            return (joblist::CHAR4NULL == *val);

        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return (joblist::DATENULL == *val);

        case CalpontSystemCatalog::UINT:
        case CalpontSystemCatalog::UMEDINT:
            return (joblist::UINTNULL == *val);

        default:
            break;
    }

    return (joblist::INTNULL == *val);
}

template<>
inline bool isNullVal<2>(uint8_t type, const uint8_t* ival)
{
    const uint16_t* val = reinterpret_cast<const uint16_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return (joblist::CHAR2NULL == *val);

        case CalpontSystemCatalog::USMALLINT:
            return (joblist::USMALLINTNULL == *val);

        default:
            break;
    }

    return (joblist::SMALLINTNULL == *val);
}

template<>
inline bool isNullVal<1>(uint8_t type, const uint8_t* ival)
{
    const uint8_t* val = reinterpret_cast<const uint8_t*>(ival);

    switch (type)
    {
        case CalpontSystemCatalog::CHAR:
        case CalpontSystemCatalog::VARCHAR:
        case CalpontSystemCatalog::BLOB:
        case CalpontSystemCatalog::TEXT:
        case CalpontSystemCatalog::DATE:
        case CalpontSystemCatalog::DATETIME:
        case CalpontSystemCatalog::TIMESTAMP:
        case CalpontSystemCatalog::TIME:
            return (*val == joblist::CHAR1NULL);

        case CalpontSystemCatalog::UTINYINT:
            return (joblist::UTINYINTNULL == *val);

        default:
            break;
    }

    return (*val == joblist::TINYINTNULL);
}

/* A generic isNullVal */
inline bool isNullVal(uint32_t length, uint8_t type, const uint8_t* val8)
{
    switch (length)
    {
        case 16:
            return isNullVal<16>(type, val8);

        case 8:
            return isNullVal<8>(type, val8);

        case 4:
            return isNullVal<4>(type, val8);

        case 2:
            return isNullVal<2>(type, val8);

        case 1:
            return isNullVal<1>(type, val8);
    };
    std::cout << __func__ << " WARNING!!! Not implemented for " << length << " bytes data types." << std::endl;
 
    return false;
}

// Set the minimum and maximum in the return header if we will be doing a block scan and
// we are dealing with a type that is comparable as a 64 bit integer.  Subsequent calls can then
// skip this block if the value being searched is outside of the Min/Max range.
inline bool isMinMaxValid(const NewColRequestHeader* in)
{
    if (in->NVALS != 0)
    {
        return false;
    }
    else
    {
        switch (in->colType.DataType)
        {
            case CalpontSystemCatalog::CHAR:
                return !in->colType.isDict();

            case CalpontSystemCatalog::VARCHAR:
            case CalpontSystemCatalog::BLOB:
            case CalpontSystemCatalog::TEXT:
                return !in->colType.isDict();

            case CalpontSystemCatalog::TINYINT:
            case CalpontSystemCatalog::SMALLINT:
            case CalpontSystemCatalog::MEDINT:
            case CalpontSystemCatalog::INT:
            case CalpontSystemCatalog::DATE:
            case CalpontSystemCatalog::BIGINT:
            case CalpontSystemCatalog::DATETIME:
            case CalpontSystemCatalog::TIME:
            case CalpontSystemCatalog::TIMESTAMP:
            case CalpontSystemCatalog::UTINYINT:
            case CalpontSystemCatalog::USMALLINT:
            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::UBIGINT:
                return true;

            case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
                return (in->colType.DataSize <= datatypes::MAXDECIMALWIDTH);

            default:
                return false;
        }
    }
}


inline bool colCompare(int64_t val1, int64_t val2, uint8_t COP, uint8_t rf,
                       const ColRequestHeaderDataType &typeHolder, uint8_t width,
                       bool isNull = false)
{
    uint8_t type = typeHolder.DataType;
// 	cout << "comparing " << hex << val1 << " to " << val2 << endl;

    if (COMPARE_NIL == COP) return false;

    //@bug 425 added isNull condition
    else if ( !isNull && (type == CalpontSystemCatalog::FLOAT || type == CalpontSystemCatalog::DOUBLE))
    {
        double dVal1, dVal2;

        if (type == CalpontSystemCatalog::FLOAT)
        {
            dVal1 = *((float*) &val1);
            dVal2 = *((float*) &val2);
        }
        else
        {
            dVal1 = *((double*) &val1);
            dVal2 = *((double*) &val2);
        }

        return colCompare_(dVal1, dVal2, COP);
    }

    else if ( (type == CalpontSystemCatalog::CHAR || type == CalpontSystemCatalog::VARCHAR ||
               type == CalpontSystemCatalog::TEXT) && !isNull )
    {
        if (COP & COMPARE_LIKE) // LIKE and NOT LIKE
        {
            utils::ConstString subject = {reinterpret_cast<const char*>(&val1), width};
            utils::ConstString pattern = {reinterpret_cast<const char*>(&val2), width};
            return typeHolder.like(COP & COMPARE_NOT, subject.rtrimZero(),
                                                      pattern.rtrimZero());
        }

        if (!rf)
        {
            // A temporary hack for xxx_nopad_bin collations
            // TODO: MCOL-4534 Improve comparison performance in 8bit nopad_bin collations
            if ((typeHolder.getCharset().state & (MY_CS_BINSORT|MY_CS_NOPAD)) ==
                (MY_CS_BINSORT|MY_CS_NOPAD))
              return colCompare_(order_swap(val1), order_swap(val2), COP);
            utils::ConstString s1 = {reinterpret_cast<const char*>(&val1), width};
            utils::ConstString s2 = {reinterpret_cast<const char*>(&val2), width};
            return colCompareStr(typeHolder, COP, s1.rtrimZero(), s2.rtrimZero());
        }
        else
            return colStrCompare_(order_swap(val1), order_swap(val2), COP, rf);
    }

    /* isNullVal should work on the normalized value on little endian machines */
    else
    {
        bool val2Null = isNullVal(width, type, (uint8_t*) &val2);

        if (isNull == val2Null || (val2Null && COP == COMPARE_NE))
            return colCompare_(val1, val2, COP, rf);
        else
            return false;
    }
}

inline bool colCompare(int128_t val1, int128_t val2, uint8_t COP, uint8_t rf, int type, uint8_t width, bool isNull = false)
{
    if (COMPARE_NIL == COP) return false;

    /* isNullVal should work on the normalized value on little endian machines */
    bool val2Null = isNullVal(width, type, (uint8_t*) &val2);

    if (isNull == val2Null || (val2Null && COP == COMPARE_NE))
        return colCompare_(val1, val2, COP, rf);
    else
        return false;
}

inline bool colCompareUnsigned(uint64_t val1, uint64_t val2, uint8_t COP, uint8_t rf, int type, uint8_t width, bool isNull = false)
{
    // 	cout << "comparing unsigned" << hex << val1 << " to " << val2 << endl;

    if (COMPARE_NIL == COP) return false;

    /* isNullVal should work on the normalized value on little endian machines */
    bool val2Null = isNullVal(width, type, (uint8_t*) &val2);

    if (isNull == val2Null || (val2Null && COP == COMPARE_NE))
        return colCompare_(val1, val2, COP, rf);
    else
        return false;
}

inline void store(const NewColRequestHeader* in,
                  NewColResultHeader* out,
                  unsigned outSize,
                  unsigned* written,
                  uint16_t rid, const uint8_t* block8)
{
    uint8_t* out8 = reinterpret_cast<uint8_t*>(out);

    if (in->OutputType & OT_RID)
    {
#ifdef PRIM_DEBUG

        if (*written + 2 > outSize)
        {
            logIt(35, 1);
            throw logic_error("PrimitiveProcessor::store(): output buffer is too small");
        }

#endif
        out->RidFlags |= (1 << (rid >> 9)); // set the (row/512)'th bit
        memcpy(&out8[*written], &rid, 2);
        *written += 2;
    }

    if (in->OutputType & OT_TOKEN || in->OutputType & OT_DATAVALUE)
    {
#ifdef PRIM_DEBUG

        if (*written + in->colType.DataSize > outSize)
        {
            logIt(35, 2);
            throw logic_error("PrimitiveProcessor::store(): output buffer is too small");
        }

#endif

        void* ptr1 = &out8[*written];
        const uint8_t* ptr2 = &block8[0];

        switch (in->colType.DataSize)
        {
            case 32:
                std::cout << __func__ << " WARNING!!! Not implemented for 32 byte data types." << std::endl;
                break;

            case 16:
                ptr2 += (rid << 4);
                memcpy(ptr1, ptr2, 16);
                break;

            default:
                std::cout << __func__ << " WARNING!!! unspecified column width." << std::endl;
                // fallthrough

            case 8:
                ptr2 += (rid << 3);
                memcpy(ptr1, ptr2, 8);
                break;

            case 4:
                ptr2 += (rid << 2);
                memcpy(ptr1, ptr2, 4);
                break;

            case 2:
                ptr2 += (rid << 1);
                memcpy(ptr1, ptr2, 2);
                break;

            case 1:
                ptr2 += (rid << 0);
                memcpy(ptr1, ptr2, 1);
                break;
        }

        *written += in->colType.DataSize;
    }

    out->NVALS++;
}

template<int W>
inline uint64_t nextUnsignedColValue(int type,
                                     const uint16_t* ridArray,
                                     int NVALS,
                                     int* index,
                                     bool* done,
                                     bool* isNull,
                                     bool* isEmpty,
                                     uint16_t* rid,
                                     uint8_t OutputType, uint8_t* val8, unsigned itemsPerBlk)
{
    const uint8_t* vp = 0;

    if (ridArray == NULL)
    {
        while (static_cast<unsigned>(*index) < itemsPerBlk &&
                isEmptyVal<W>(type, &val8[*index * W]) &&
                (OutputType & OT_RID))
        {
            (*index)++;
        }

        if (static_cast<unsigned>(*index) >= itemsPerBlk)
        {
            *done = true;
            return 0;
        }

        vp = &val8[*index * W];
        *isNull = isNullVal<W>(type, vp);
        *isEmpty = isEmptyVal<W>(type, vp);
        *rid = (*index)++;
    }
    else
    {
        while (*index < NVALS &&
                isEmptyVal<W>(type, &val8[ridArray[*index] * W]))
        {
            (*index)++;
        }

        if (*index >= NVALS)
        {
            *done = true;
            return 0;
        }

        vp = &val8[ridArray[*index] * W];
        *isNull = isNullVal<W>(type, vp);
        *isEmpty = isEmptyVal<W>(type, vp);
        *rid = ridArray[(*index)++];
    }

    // at this point, nextRid is the index to return, and index is...
    //   if RIDs are not specified, nextRid + 1,
    //	 if RIDs are specified, it's the next index in the rid array.
    //Bug 838, tinyint null problem
    switch (W)
    {
        case 1:
            return reinterpret_cast<uint8_t*> (val8)[*rid];

        case 2:
            return reinterpret_cast<uint16_t*>(val8)[*rid];

        case 4:
            return reinterpret_cast<uint32_t*>(val8)[*rid];

        case 8:
            return reinterpret_cast<uint64_t*>(val8)[*rid];

        default:
            logIt(33, W);

#ifdef PRIM_DEBUG
            throw logic_error("PrimitiveProcessor::nextColValue() bad width");
#endif
            return -1;
    }
}
template<int W>
inline uint8_t* nextBinColValue(int type,
                                     const uint16_t* ridArray,
                                     int NVALS,
                                     int* index,
                                     bool* done,
                                     bool* isNull,
                                     bool* isEmpty,
                                     uint16_t* rid,
                                     uint8_t OutputType, uint8_t* val8, unsigned itemsPerBlk)
{
    if (ridArray == NULL)
    {
        while (static_cast<unsigned>(*index) < itemsPerBlk &&
                isEmptyVal<W>(type, &val8[*index * W]) &&
                (OutputType & OT_RID))
        {
            (*index)++;
        }


        if (static_cast<unsigned>(*index) >= itemsPerBlk)
        {
            *done = true;
            return NULL;
        }
        *rid = (*index)++;
    }
    else
    {
        while (*index < NVALS &&
            isEmptyVal<W>(type, &val8[ridArray[*index] * W]))
        {
            (*index)++;
        }

        if (*index >= NVALS)
        {
            *done = true;
            return NULL;
        }
        *rid = ridArray[(*index)++];
    }

    uint32_t curValueOffset = *rid * W;

    *isNull = isNullVal<W>(type, &val8[curValueOffset]);
    *isEmpty = isEmptyVal<W>(type, &val8[curValueOffset]);
    //cout << "nextColBinValue " << *index <<  " rowid " << *rid << endl;
    // at this point, nextRid is the index to return, and index is...
    //   if RIDs are not specified, nextRid + 1,
    //	 if RIDs are specified, it's the next index in the rid array.
    return &val8[curValueOffset];

#ifdef PRIM_DEBUG
            throw logic_error("PrimitiveProcessor::nextColBinValue() bad width");
#endif
            return NULL;
}


template<int W>
inline int64_t nextColValue(int type,
                            const uint16_t* ridArray,
                            int NVALS,
                            int* index,
                            bool* done,
                            bool* isNull,
                            bool* isEmpty,
                            uint16_t* rid,
                            uint8_t OutputType, uint8_t* val8, unsigned itemsPerBlk)
{
    const uint8_t* vp = 0;

    if (ridArray == NULL)
    {
        while (static_cast<unsigned>(*index) < itemsPerBlk &&
                isEmptyVal<W>(type, &val8[*index * W]) &&
                (OutputType & OT_RID))
        {
            (*index)++;
        }

        if (static_cast<unsigned>(*index) >= itemsPerBlk)
        {
            *done = true;
            return 0;
        }

        vp = &val8[*index * W];
        *isNull = isNullVal<W>(type, vp);
        *isEmpty = isEmptyVal<W>(type, vp);
        *rid = (*index)++;
    }
    else
    {
        while (*index < NVALS &&
                isEmptyVal<W>(type, &val8[ridArray[*index] * W]))
        {
            (*index)++;
        }

        if (*index >= NVALS)
        {
            *done = true;
            return 0;
        }

        vp = &val8[ridArray[*index] * W];
        *isNull = isNullVal<W>(type, vp);
        *isEmpty = isEmptyVal<W>(type, vp);
        *rid = ridArray[(*index)++];
    }

    // at this point, nextRid is the index to return, and index is...
    //   if RIDs are not specified, nextRid + 1,
    //	 if RIDs are specified, it's the next index in the rid array.
    //Bug 838, tinyint null problem
    switch (W)
    {
        case 1:
            return reinterpret_cast<int8_t*> (val8)[*rid];

        case 2:
            return reinterpret_cast<int16_t*>(val8)[*rid];

        case 4:
#if 0
            if (type == CalpontSystemCatalog::FLOAT)
            {
                // convert the float to a 64-bit type, return that w/o conversion
                int32_t* val32 = reinterpret_cast<int32_t*>(val8);
                double dTmp;
                dTmp = (double) * ((float*) &val32[*rid]);
                return *((int64_t*) &dTmp);
            }
            else
            {
                return reinterpret_cast<int32_t*>(val8)[*rid];
            }

#else
            return reinterpret_cast<int32_t*>(val8)[*rid];
#endif

        case 8:
            return reinterpret_cast<int64_t*>(val8)[*rid];

        default:
            logIt(33, W);

#ifdef PRIM_DEBUG
            throw logic_error("PrimitiveProcessor::nextColValue() bad width");
#endif
            return -1;
    }
}


// done should be init'd to false and
// index should be init'd to 0 on the first call
// done == true when there are no more elements to return.
inline uint64_t nextUnsignedColValueHelper(int type,
        int width,
        const uint16_t* ridArray,
        int NVALS,
        int* index,
        bool* done,
        bool* isNull,
        bool* isEmpty,
        uint16_t* rid,
        uint8_t OutputType, uint8_t* val8, unsigned itemsPerBlk)
{
    switch (width)
    {
        case 8:
            return nextUnsignedColValue<8>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
                                           itemsPerBlk);

        case 4:
            return nextUnsignedColValue<4>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
                                           itemsPerBlk);

        case 2:
            return nextUnsignedColValue<2>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
                                           itemsPerBlk);

        case 1:
            return nextUnsignedColValue<1>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
                                           itemsPerBlk);

        default:
            idbassert(0);
    }

    /*NOTREACHED*/
    return 0;
}

// done should be init'd to false and
// index should be init'd to 0 on the first call
// done == true when there are no more elements to return.
inline int64_t nextColValueHelper(int type,
                                  int width,
                                  const uint16_t* ridArray,
                                  int NVALS,
                                  int* index,
                                  bool* done,
                                  bool* isNull,
                                  bool* isEmpty,
                                  uint16_t* rid,
                                  uint8_t OutputType, uint8_t* val8, unsigned itemsPerBlk)
{
    switch (width)
    {
        case 8:
            return nextColValue<8>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
                                   itemsPerBlk);

        case 4:
            return nextColValue<4>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
                                   itemsPerBlk);

        case 2:
            return nextColValue<2>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
                                   itemsPerBlk);

        case 1:
            return nextColValue<1>(type, ridArray, NVALS, index, done, isNull, isEmpty, rid, OutputType, val8,
                                   itemsPerBlk);

        default:
            idbassert(0);
    }

    /*NOTREACHED*/
    return 0;
}


template<int W>
inline void p_Col_ridArray(NewColRequestHeader* in,
                           NewColResultHeader* out,
                           unsigned outSize,
                           unsigned* written, int* block, Stats* fStatsPtr, unsigned itemsPerBlk,
                           boost::shared_ptr<ParsedColumnFilter> parsedColumnFilter)
{
    uint16_t* ridArray = 0;
    uint8_t* in8 = reinterpret_cast<uint8_t*>(in);
    const uint8_t filterSize = sizeof(uint8_t) + sizeof(uint8_t) + W;

    if (in->NVALS > 0)
        ridArray = reinterpret_cast<uint16_t*>(&in8[sizeof(NewColRequestHeader) +
                                                                           (in->NOPS * filterSize)]);

    if (ridArray && 1 == in->sort )
    {
        qsort(ridArray, in->NVALS, sizeof(uint16_t), compareBlock<uint16_t>);

        if (fStatsPtr)
#ifdef _MSC_VER
            fStatsPtr->markEvent(in->LBID, GetCurrentThreadId(), in->hdr.SessionID, 'O');

#else
            fStatsPtr->markEvent(in->LBID, pthread_self(), in->hdr.SessionID, 'O');
#endif
    }

    // Set boolean indicating whether to capture the min and max values.
    out->ValidMinMax = isMinMaxValid(in);

    if (out->ValidMinMax)
    {
        if (isUnsigned((CalpontSystemCatalog::ColDataType)in->colType.DataType))
        {
            out->Min = static_cast<int64_t>(numeric_limits<uint64_t>::max());
            out->Max = 0;
        }
        else
        {
            out->Min = numeric_limits<int64_t>::max();
            out->Max = numeric_limits<int64_t>::min();
        }
    }
    else
    {
        out->Min = 0;
        out->Max = 0;
    }

    const ColArgs* args = NULL;
    int64_t val = 0;
    uint64_t uval = 0;
    int nextRidIndex = 0, argIndex = 0;
    bool done = false, cmp = false, isNull = false, isEmpty = false;
    uint16_t rid = 0;
    prestored_set_t::const_iterator it;

    int64_t* std_argVals = (int64_t*)alloca(in->NOPS * sizeof(int64_t));
    uint8_t* std_cops = (uint8_t*)alloca(in->NOPS * sizeof(uint8_t));
    uint8_t* std_rfs = (uint8_t*)alloca(in->NOPS * sizeof(uint8_t));
    int64_t* argVals = NULL;
    uint64_t* uargVals = NULL;
    uint8_t* cops = NULL;
    uint8_t* rfs = NULL;

    // no pre-parsed column filter is set, parse the filter in the message
    if (parsedColumnFilter.get() == NULL)
    {
        if (isUnsigned((CalpontSystemCatalog::ColDataType)in->colType.DataType))
        {
            uargVals = reinterpret_cast<uint64_t*>(std_argVals);
            cops = std_cops;
            rfs = std_rfs;

            for (argIndex = 0; argIndex < in->NOPS; argIndex++)
            {
                args = reinterpret_cast<const ColArgs*>(&in8[sizeof(NewColRequestHeader) +
                                                                                    (argIndex * filterSize)]);
                cops[argIndex] = args->COP;
                rfs[argIndex] = args->rf;

                switch (W)
                {
                    case 1:
                        uargVals[argIndex] = *reinterpret_cast<const uint8_t*>(args->val);
                        break;

                    case 2:
                        uargVals[argIndex] = *reinterpret_cast<const uint16_t*>(args->val);
                        break;

                    case 4:
                        uargVals[argIndex] = *reinterpret_cast<const uint32_t*>(args->val);
                        break;

                    case 8:
                        uargVals[argIndex] = *reinterpret_cast<const uint64_t*>(args->val);
                        break;
                }
            }
        }
        else
        {
            argVals = std_argVals;
            cops = std_cops;
            rfs = std_rfs;

            for (argIndex = 0; argIndex < in->NOPS; argIndex++)
            {
                args = reinterpret_cast<const ColArgs*>(&in8[sizeof(NewColRequestHeader) +
                                                                                    (argIndex * filterSize)]);
                cops[argIndex] = args->COP;
                rfs[argIndex] = args->rf;

                switch (W)
                {
                    case 1:
                        argVals[argIndex] = args->val[0];
                        break;

                    case 2:
                        argVals[argIndex] = *reinterpret_cast<const int16_t*>(args->val);
                        break;

                    case 4:
#if 0
                        if (in->colType.DataType == CalpontSystemCatalog::FLOAT)
                        {
                            double dTmp;

                            dTmp = (double) * ((const float*) args->val);
                            argVals[argIndex] = *((int64_t*) &dTmp);
                        }
                        else
                            argVals[argIndex] = *reinterpret_cast<const int32_t*>(args->val);

#else
                        argVals[argIndex] = *reinterpret_cast<const int32_t*>(args->val);
#endif
                        break;

                    case 8:
                        argVals[argIndex] = *reinterpret_cast<const int64_t*>(args->val);
                        break;
                }
            }
        }
    }
    // we have a pre-parsed filter, and it's in the form of op and value arrays
    else if (parsedColumnFilter->columnFilterMode == TWO_ARRAYS)
    {
        argVals = parsedColumnFilter->prestored_argVals.get();
        uargVals = reinterpret_cast<uint64_t*>(parsedColumnFilter->prestored_argVals.get());
        cops = parsedColumnFilter->prestored_cops.get();
        rfs = parsedColumnFilter->prestored_rfs.get();
    }

    // else we have a pre-parsed filter, and it's an unordered set for quick == comparisons

    if (isUnsigned((CalpontSystemCatalog::ColDataType)in->colType.DataType))
    {
        uval = nextUnsignedColValue<W>(in->colType.DataType, ridArray, in->NVALS, &nextRidIndex, &done, &isNull,
                                       &isEmpty, &rid, in->OutputType, reinterpret_cast<uint8_t*>(block), itemsPerBlk);
    }
    else
    {
        val = nextColValue<W>(in->colType.DataType, ridArray, in->NVALS, &nextRidIndex, &done, &isNull,
                              &isEmpty, &rid, in->OutputType, reinterpret_cast<uint8_t*>(block), itemsPerBlk);
    }

    while (!done)
    {
        if (cops == NULL)    // implies parsedColumnFilter && columnFilterMode == SET
        {
            /* bug 1920: ignore NULLs in the set and in the column data */
            if (!(isNull && in->BOP == BOP_AND))
            {
                if (isUnsigned((CalpontSystemCatalog::ColDataType)in->colType.DataType))
                {
                    it = parsedColumnFilter->prestored_set->find(*reinterpret_cast<int64_t*>(&uval));
                }
                else
                {
                    it = parsedColumnFilter->prestored_set->find(val);
                }

                if (in->BOP == BOP_OR)
                {
                    // assume COP == COMPARE_EQ
                    if (it != parsedColumnFilter->prestored_set->end())
                    {
                        store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t*>(block));
                    }
                }
                else if (in->BOP == BOP_AND)
                {
                    // assume COP == COMPARE_NE
                    if (it == parsedColumnFilter->prestored_set->end())
                    {
                        store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t*>(block));
                    }
                }
            }
        }
        else
        {
            for (argIndex = 0; argIndex < in->NOPS; argIndex++)
            {
                if (isUnsigned((CalpontSystemCatalog::ColDataType)in->colType.DataType))
                {
                    cmp = colCompareUnsigned(uval, uargVals[argIndex], cops[argIndex],
                                             rfs[argIndex], in->colType.DataType, W, isNull);
                }
                else
                {
                    cmp = colCompare(val, argVals[argIndex], cops[argIndex],
                                     rfs[argIndex], in->colType, W, isNull);
                }

                if (in->NOPS == 1)
                {
                    if (cmp == true)
                    {
                        store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t*>(block));
                    }

                    break;
                }
                else if (in->BOP == BOP_AND && cmp == false)
                {
                    break;
                }
                else if (in->BOP == BOP_OR && cmp == true)
                {
                    store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t*>(block));
                    break;
                }
            }

            if ((argIndex == in->NOPS && in->BOP == BOP_AND) || in->NOPS == 0)
            {
                store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t*>(block));
            }
        }

        // Set the min and max if necessary.  Ignore nulls.
        if (out->ValidMinMax && !isNull && !isEmpty)
        {

            if (in->colType.DataType == CalpontSystemCatalog::CHAR ||
                in->colType.DataType == CalpontSystemCatalog::VARCHAR ||
                in->colType.DataType == CalpontSystemCatalog::BLOB ||
                in->colType.DataType == CalpontSystemCatalog::TEXT )
            {
                if (colCompare(out->Min, val, COMPARE_GT, false, in->colType, W))
                    out->Min = val;

                if (colCompare(out->Max, val, COMPARE_LT, false, in->colType, W))
                    out->Max = val;
            }
            else if (isUnsigned((CalpontSystemCatalog::ColDataType)in->colType.DataType))
            {
                if (static_cast<uint64_t>(out->Min) > uval)
                    out->Min = static_cast<int64_t>(uval);

                if (static_cast<uint64_t>(out->Max) < uval)
                    out->Max = static_cast<int64_t>(uval);;
            }
            else
            {
                if (out->Min > val)
                    out->Min = val;

                if (out->Max < val)
                    out->Max = val;
            }
        }

        if (isUnsigned((CalpontSystemCatalog::ColDataType)in->colType.DataType))
        {
            uval = nextUnsignedColValue<W>(in->colType.DataType, ridArray, in->NVALS, &nextRidIndex, &done,
                                           &isNull, &isEmpty, &rid, in->OutputType, reinterpret_cast<uint8_t*>(block),
                                           itemsPerBlk);
        }
        else
        {
            val = nextColValue<W>(in->colType.DataType, ridArray, in->NVALS, &nextRidIndex, &done,
                                  &isNull, &isEmpty, &rid, in->OutputType, reinterpret_cast<uint8_t*>(block),
                                  itemsPerBlk);
        }
    }

    if (fStatsPtr)
#ifdef _MSC_VER
        fStatsPtr->markEvent(in->LBID, GetCurrentThreadId(), in->hdr.SessionID, 'K');

#else
        fStatsPtr->markEvent(in->LBID, pthread_self(), in->hdr.SessionID, 'K');
#endif
}

// There are number of hardcoded type-dependant objects
// that effectively makes this template int128-based only.
// Use type based template method for Min,Max values.
// prestored_set_128 must be a template with a type arg.
template<int W, typename T>
inline void p_Col_bin_ridArray(NewColRequestHeader* in,
                           NewColResultHeader* out,
                           unsigned outSize,
                           unsigned* written, int* block, Stats* fStatsPtr, unsigned itemsPerBlk,
                           boost::shared_ptr<ParsedColumnFilter> parsedColumnFilter)
{
    uint16_t* ridArray = 0;
    uint8_t* in8 = reinterpret_cast<uint8_t*>(in);
    const uint8_t filterSize = sizeof(uint8_t) + sizeof(uint8_t) + W;

    if (in->NVALS > 0)
        ridArray = reinterpret_cast<uint16_t*>(&in8[sizeof(NewColRequestHeader) +
                                                                           (in->NOPS * filterSize)]);

    if (ridArray && 1 == in->sort )
    {
        qsort(ridArray, in->NVALS, sizeof(uint16_t), compareBlock<uint16_t>);

        if (fStatsPtr)
#ifdef _MSC_VER
            fStatsPtr->markEvent(in->LBID, GetCurrentThreadId(), in->hdr.SessionID, 'O');

#else
            fStatsPtr->markEvent(in->LBID, pthread_self(), in->hdr.SessionID, 'O');
#endif
    }

    // Set boolean indicating whether to capture the min and max values.
    out->ValidMinMax = isMinMaxValid(in);

    if (out->ValidMinMax)
    {
        // Assume that isUnsigned returns true for 8-bytes DTs only
        if (isUnsigned((CalpontSystemCatalog::ColDataType)in->colType.DataType))
        {
            out->Min = -1;
            out->Max = 0;
        }
        else
        {
            out->Min = datatypes::Decimal::maxInt128;
            out->Max = datatypes::Decimal::minInt128;
        }
    }
    else
    {
        out->Min = 0;
        out->Max = 0;
    }

    typedef char binWtype [W];

    const ColArgs* args = NULL;
    binWtype* bval;
    int nextRidIndex = 0, argIndex = 0;
    bool done = false, cmp = false, isNull = false, isEmpty = false;
    uint16_t rid = 0;
    prestored_set_t_128::const_iterator it;

    binWtype* argVals = (binWtype*)alloca(in->NOPS * W);
    uint8_t* std_cops = (uint8_t*)alloca(in->NOPS * sizeof(uint8_t));
    uint8_t* std_rfs = (uint8_t*)alloca(in->NOPS * sizeof(uint8_t));
    uint8_t* cops = NULL;
    uint8_t* rfs = NULL;

    // no pre-parsed column filter is set, parse the filter in the message
    if (parsedColumnFilter.get() == NULL) {

        cops = std_cops;
        rfs = std_rfs;

        for (argIndex = 0; argIndex < in->NOPS; argIndex++) {
            args = reinterpret_cast<const ColArgs*> (&in8[sizeof (NewColRequestHeader) +
                    (argIndex * filterSize)]);
            cops[argIndex] = args->COP;
            rfs[argIndex] = args->rf;

            memcpy(argVals[argIndex],args->val, W);
        }
    }
    // we have a pre-parsed filter, and it's in the form of op and value arrays
    else if (parsedColumnFilter->columnFilterMode == TWO_ARRAYS)
    {
        argVals = (binWtype*) parsedColumnFilter->prestored_argVals128.get();
        cops = parsedColumnFilter->prestored_cops.get();
        rfs = parsedColumnFilter->prestored_rfs.get();
    }

    // else we have a pre-parsed filter, and it's an unordered set for quick == comparisons

    bval = (binWtype*)nextBinColValue<W>(in->colType.DataType, ridArray, in->NVALS, &nextRidIndex, &done, &isNull,
                &isEmpty, &rid, in->OutputType, reinterpret_cast<uint8_t*>(block), itemsPerBlk);

    T val;

    while (!done)
    {
        val = *reinterpret_cast<T*>(bval);

        if (cops == NULL)    // implies parsedColumnFilter && columnFilterMode == SET
        {
            /* bug 1920: ignore NULLs in the set and in the column data */
            if (!(isNull && in->BOP == BOP_AND))
            {

                it = parsedColumnFilter->prestored_set_128->find(val);


                if (in->BOP == BOP_OR)
                {
                    // assume COP == COMPARE_EQ
                    if (it != parsedColumnFilter->prestored_set_128->end())
                    {
                        store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t*>(block));
                    }
                }
                else if (in->BOP == BOP_AND)
                {
                    // assume COP == COMPARE_NE
                    if (it == parsedColumnFilter->prestored_set_128->end())
                    {
                        store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t*>(block));
                    }
                }
            }
        }
        else
        {
            for (argIndex = 0; argIndex < in->NOPS; argIndex++)
            {
                T filterVal = *reinterpret_cast<T*>(argVals[argIndex]);

                cmp = colCompare(val, filterVal, cops[argIndex],
                                 rfs[argIndex], in->colType.DataType, W, isNull);

                if (in->NOPS == 1)
                {
                    if (cmp == true)
                    {
                        store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t*>(block));
                    }
                    break;
                }
                else if (in->BOP == BOP_AND && cmp == false)
                {
                    break;
                }
                else if (in->BOP == BOP_OR && cmp == true)
                {
                    store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t*>(block));
                    break;
                }
            }

            if ((argIndex == in->NOPS && in->BOP == BOP_AND) || in->NOPS == 0)
            {
                store(in, out, outSize, written, rid, reinterpret_cast<const uint8_t*>(block));
            }
        }

        // Set the min and max if necessary.  Ignore nulls.
        if (out->ValidMinMax && !isNull && !isEmpty)
        {

            if (in->colType.DataType == CalpontSystemCatalog::CHAR ||
                in->colType.DataType == CalpontSystemCatalog::VARCHAR)
            {
                // !!! colCompare is overloaded with int128_t only yet.
                if (colCompare(out->Min, val, COMPARE_GT, false, in->colType, W))
                {
                    out->Min = val;
                }

                if (colCompare(out->Max, val, COMPARE_LT, false, in->colType, W))
                {
                    out->Max = val;
                }
            }
            else
            {
                if (out->Min > val)
                {
                    out->Min = val;
                }

                if (out->Max < val)
                {
                    out->Max = val;
                }
            }
        }

        bval = (binWtype*)nextBinColValue<W>(in->colType.DataType, ridArray, in->NVALS, &nextRidIndex, &done, &isNull,
            &isEmpty, &rid, in->OutputType, reinterpret_cast<uint8_t*>(block), itemsPerBlk);

    }

    if (fStatsPtr)
#ifdef _MSC_VER
        fStatsPtr->markEvent(in->LBID, GetCurrentThreadId(), in->hdr.SessionID, 'K');

#else
        fStatsPtr->markEvent(in->LBID, pthread_self(), in->hdr.SessionID, 'K');
#endif
}

} //namespace anon

namespace primitives
{

void PrimitiveProcessor::p_Colaaa(NewColRequestHeader* in, NewColResultHeader* out,
                               unsigned outSize, unsigned* written)
{
    void *outp = static_cast<void*>(out);
    memcpy(outp, in, sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
    out->NVALS = 0;
    out->LBID = in->LBID;
    out->ism.Command = COL_RESULTS;
    out->OutputType = in->OutputType;
    out->RidFlags = 0;
    *written = sizeof(NewColResultHeader);
    unsigned itemsPerBlk = 0;

    if (logicalBlockMode)
        itemsPerBlk = BLOCK_SIZE;
    else
        itemsPerBlk = BLOCK_SIZE / in->colType.DataSize;

    //...Initialize I/O counts;
    out->CacheIO    = 0;
    out->PhysicalIO = 0;

#if 0

    // short-circuit the actual block scan for testing
    if (out->LBID >= 802816)
    {
        out->ValidMinMax = false;
        out->Min = 0;
        out->Max = 0;
        return;
    }

#endif

    if (fStatsPtr)
#ifdef _MSC_VER
        fStatsPtr->markEvent(in->LBID, GetCurrentThreadId(), in->hdr.SessionID, 'B');

#else
        fStatsPtr->markEvent(in->LBID, pthread_self(), in->hdr.SessionID, 'B');
#endif

    switch (in->colType.DataSize)
    {
        case 8:
            p_Col_ridArray<8>(in, out, outSize, written, block, fStatsPtr, itemsPerBlk, parsedColumnFilter);
            break;

        case 4:
            p_Col_ridArray<4>(in, out, outSize, written, block, fStatsPtr, itemsPerBlk, parsedColumnFilter);
            break;

        case 2:
            p_Col_ridArray<2>(in, out, outSize, written, block, fStatsPtr, itemsPerBlk, parsedColumnFilter);
            break;

        case 1:
            p_Col_ridArray<1>(in, out, outSize, written, block, fStatsPtr, itemsPerBlk, parsedColumnFilter);
            break;

        case 16:
            p_Col_bin_ridArray<16, int128_t>(in, out, outSize, written, block, fStatsPtr, itemsPerBlk, parsedColumnFilter);
            break;

        case 32:
            std::cout << __func__ << " WARNING!!! Not implemented for 32 byte data types." << std::endl;
            // fallthrough

        default:
            idbassert(0);
            break;
    }

    if (fStatsPtr)
#ifdef _MSC_VER
        fStatsPtr->markEvent(in->LBID, GetCurrentThreadId(), in->hdr.SessionID, 'C');

#else
        fStatsPtr->markEvent(in->LBID, pthread_self(), in->hdr.SessionID, 'C');
#endif
}

boost::shared_ptr<ParsedColumnFilter> parseColumnFilter
(const uint8_t* filterString, uint32_t colWidth, uint32_t colType, uint32_t filterCount,
 uint32_t BOP)
{
    boost::shared_ptr<ParsedColumnFilter> ret;
    uint32_t argIndex;
    const ColArgs* args;
    bool convertToSet = true;

    if (filterCount == 0)
        return ret;

    ret.reset(new ParsedColumnFilter());

    ret->columnFilterMode = TWO_ARRAYS;
    if (datatypes::isWideDecimalType(
        (CalpontSystemCatalog::ColDataType)colType, colWidth))
        ret->prestored_argVals128.reset(new int128_t[filterCount]);
    else
        ret->prestored_argVals.reset(new int64_t[filterCount]);
    ret->prestored_cops.reset(new uint8_t[filterCount]);
    ret->prestored_rfs.reset(new uint8_t[filterCount]);

    /*
    for (unsigned ii = 0; ii < filterCount; ii++)
    {
    	ret->prestored_argVals[ii] = 0;
    	ret->prestored_cops[ii] = 0;
    	ret->prestored_rfs[ii] = 0;
    }
    */

    const uint8_t filterSize = sizeof(uint8_t) + sizeof(uint8_t) + colWidth;

    /*  Decide which structure to use.  I think the only cases where we can use the set
    	are when NOPS > 1, BOP is OR, and every COP is ==,
    	and when NOPS > 1, BOP is AND, and every COP is !=.

    	Parse the filter predicates and insert them into argVals and cops.
    	If there were no predicates that violate the condition for using a set,
    	insert argVals into a set.
    */
    if (filterCount == 1)
        convertToSet = false;

    for (argIndex = 0; argIndex < filterCount; argIndex++)
    {
        args = reinterpret_cast<const ColArgs*>(filterString + (argIndex * filterSize));
        ret->prestored_cops[argIndex] = args->COP;
        ret->prestored_rfs[argIndex] = args->rf;

        if ((BOP == BOP_OR && args->COP != COMPARE_EQ) ||
                (BOP == BOP_AND && args->COP != COMPARE_NE) ||
                (args->COP == COMPARE_NIL))
            convertToSet = false;

        if (isUnsigned((CalpontSystemCatalog::ColDataType)colType))
        {
            switch (colWidth)
            {
                case 1:
                    ret->prestored_argVals[argIndex] = *reinterpret_cast<const uint8_t*>(args->val);
                    break;

                case 2:
                    ret->prestored_argVals[argIndex] = *reinterpret_cast<const uint16_t*>(args->val);
                    break;

                case 4:
                    ret->prestored_argVals[argIndex] = *reinterpret_cast<const uint32_t*>(args->val);
                    break;

                case 8:
                    ret->prestored_argVals[argIndex] = *reinterpret_cast<const uint64_t*>(args->val);
                    break;
            }
        }
        else
        {
            switch (colWidth)
            {
                case 1:
                    ret->prestored_argVals[argIndex] = args->val[0];
                    break;

                case 2:
                    ret->prestored_argVals[argIndex] = *reinterpret_cast<const int16_t*>(args->val);
                    break;

                case 4:
#if 0
                    if (colType == CalpontSystemCatalog::FLOAT)
                    {
                        double dTmp;

                        dTmp = (double) * ((const float*) args->val);
                        ret->prestored_argVals[argIndex] = *((int64_t*) &dTmp);
                    }
                    else
                        ret->prestored_argVals[argIndex] =
                            *reinterpret_cast<const int32_t*>(args->val);

#else
                    ret->prestored_argVals[argIndex] = *reinterpret_cast<const int32_t*>(args->val);
#endif
                    break;

                case 8:
                    ret->prestored_argVals[argIndex] = *reinterpret_cast<const int64_t*>(args->val);
                    break;

                case 16:
                {
                    datatypes::TSInt128::assignPtrPtr(&(ret->prestored_argVals128[argIndex]),
                                                            args->val);
                    break;
                }
            }
        }

// 		cout << "inserted* " << hex << ret->prestored_argVals[argIndex] << dec <<
// 		  " COP = " << (int) ret->prestored_cops[argIndex] << endl;

    }

    if (convertToSet)
    {
        ret->columnFilterMode = UNORDERED_SET;
        if (datatypes::isWideDecimalType(
            (CalpontSystemCatalog::ColDataType)colType, colWidth))
        {
            ret->prestored_set_128.reset(new prestored_set_t_128());

            // @bug 2584, use COMPARE_NIL for "= null" to allow "is null" in OR expression
            for (argIndex = 0; argIndex < filterCount; argIndex++)
                if (ret->prestored_rfs[argIndex] == 0)
                    ret->prestored_set_128->insert(ret->prestored_argVals128[argIndex]);
        }
        else
        {
            ret->prestored_set.reset(new prestored_set_t());

            // @bug 2584, use COMPARE_NIL for "= null" to allow "is null" in OR expression
            for (argIndex = 0; argIndex < filterCount; argIndex++)
                if (ret->prestored_rfs[argIndex] == 0)
                    ret->prestored_set->insert(ret->prestored_argVals[argIndex]);
        }
    }

    return ret;
}



} // namespace primitives
// vim:ts=4 sw=4:
