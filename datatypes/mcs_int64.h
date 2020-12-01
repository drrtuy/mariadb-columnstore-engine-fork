/*
   Copyright (C) 2020 MariaDB Corporation

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
   MA 02110-1301, USA. 
*/ 
#ifndef MCS_INT64_H_INCLUDED
#define MCS_INT64_H_INCLUDED

#include <limits>
#include <type_traits>
#include "dbcon/joblist/joblisttypes.h"
#include "utils/common/checks.h"

namespace datatypes
{

//    Type traits
template <typename T>
struct is_allowed_numeric_TSInt64 {
  static const bool value = false;
};

template <>
struct is_allowed_numeric_TSInt64<int64_t> {
  static const bool value = true;
};

template <>
struct is_allowed_numeric_TSInt64<int128_t> {
  static const bool value = true;
};


class TSInt64
{
  public:
    static constexpr uint8_t MAXLENGTH8BYTES = 21;
    static constexpr int64_t NullValue = joblist::BIGINTNULL;
    static constexpr int64_t EmptyValue = joblist::BIGINTEMPTYROW;

    //  A variety of ctors for aligned and unaligned arguments
    TSInt64(): value(0) { }

    //    aligned argument
    TSInt64(const int64_t& x) { value = x; }

    //    unaligned argument
    TSInt64(const int64_t* x) { value = *x; }

    //    unaligned argument
    TSInt64(const unsigned char* x)
    {
      value = *(reinterpret_cast<const int64_t*>(x));
    }

    //    Method returns max length of a string representation
    static constexpr uint8_t maxLength()
    {
      return MAXLENGTH8BYTES;
    }

    inline bool isNullImpl() const
    {
      return value == NullValue;
    }

    //    Checks if the value is Empty
    inline bool isEmptyImpl() const
    {
      return value == EmptyValue;
    }

    // operators
    template<typename T,
             typename = std::enable_if<is_allowed_numeric_TSInt64<T>::value> >
    inline bool operator<(const T& x) const
    {
      return value < static_cast<int64_t>(x);
    }

    template<typename T,
             typename = std::enable_if<is_allowed_numeric_TSInt64<T>::value> >
    inline bool operator==(const T& x) const
    {
      return value == static_cast<int64_t>(x);
    }

    inline operator double() const
    {
      return toDouble();
    }

    inline double toDouble() const
    {
      return static_cast<double>(value);
    }

    inline operator long double() const
    {
      return toLongDouble();
    }

    inline long double toLongDouble() const
    {
      return static_cast<long double>(value);
    }

    inline operator int32_t() const
    {
      if (value > static_cast<int64_t>(INT32_MAX))
          return INT32_MAX;
      if (value < static_cast<int64_t>(INT32_MIN))
          return INT32_MIN;

      return static_cast<int32_t>(value);
    }

    inline operator uint32_t() const
    {
      if (value > static_cast<int64_t>(UINT32_MAX))
          return UINT32_MAX;
      if (value < 0)
          return 0;

      return static_cast<uint32_t>(value);
    }

    inline operator uint64_t() const
    {
      if (utils::is_negative(value))
        return 0;
      return value;

      return static_cast<uint64_t>(value);
    }

    inline TSInt64 operator%(const int64_t& rhs) const
    {
      return TSInt64(value % rhs);
    }

    inline TSInt64 operator*(const TSInt64& rhs) const
    {
      return TSInt64(value * rhs.value);
    }

    inline TSInt64 operator+(const TSInt64& rhs) const
    {
      return TSInt64(value + rhs.value);
    }

    inline const int64_t& getValue() const
    {
      return value;
    }

    //    string representation of TSInt64
    std::string toString() const
    {
      return std::to_string(value);
    }

    std::string toStringWithScaleImpl(const uint8_t precision,const int8_t scale) const
    {
      char buf[MAXLENGTH8BYTES];
      // Need 19 digits maxium to hold a sum result of 18 digits decimal column.
      // We don't make a copy of value b/c we mutate its string
      // representation.
#ifndef __LP64__
      snprintf(buf, sizeof(buf), "%lld", value);
#else
      snprintf(buf, sizeof(buf), "%ld", value);
#endif

      //we want to move the last dt_scale chars right by one spot
      // to insert the dp we want to move the trailing null as well,
      // so it's really dt_scale+1 chars
      size_t l1 = strlen(buf);
      char* ptr = &buf[0];

      if (value < 0)
      {
        ptr++;
        idbassert(l1 >= 2);
        l1--;
      }

      //need to make sure we have enough leading zeros for this to work.
      //at this point scale is always > 0
      size_t l2 = 1;

      if ((unsigned)scale > l1)
      {
        const char* zeros = "00000000000000000000"; //20 0's
        size_t diff = 0;

        if (value != 0)
            diff = scale - l1; //this will always be > 0
        else
            diff = scale;

        memmove((ptr + diff), ptr, l1 + 1); //also move null
        memcpy(ptr, zeros, diff);

        if (value != 0)
            l1 = 0;
        else
              l1 = 1;
      }
      else if ((unsigned)scale == l1)
      {
        l1 = 0;
        l2 = 2;
      }
      else
      {
        l1 -= scale;
      }

      memmove((ptr + l1 + l2), (ptr + l1), scale + 1); //also move null

      if (l2 == 2)
        *(ptr + l1++) = '0';

      *(ptr + l1) = '.';
      return std::string(buf);
    }

    friend std::ostream& operator<<(std::ostream& os, const TSInt64& x)
    {
      os << x.toString();
      return os;
    }

    int64_t value;
  }; // end of class
    

} //end of namespace datatypes

#endif // MCS_TSINT64_H_INCLUDED
// vim:ts=2 sw=2:
