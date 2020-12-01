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

#include <iostream>

#include "mcs_int128.h"
#include "mcs_datatypes_constants.h"
#include "exceptclasses.h"

namespace datatypes
{
  uint8_t TSInt128::printPodParts(char* buf,
                                  const int128_t& high,
                                  const int128_t& mid,
                                  const int128_t& low) const
  {
    char* p = buf;
    // pod[0] is low 8 bytes, pod[1] is high 8 bytes
    const uint64_t* high_pod = reinterpret_cast<const uint64_t*>(&high);
    const uint64_t* mid_pod = reinterpret_cast<const uint64_t*>(&mid);
    const uint64_t* low_pod = reinterpret_cast<const uint64_t*>(&low);

    if (high_pod[0] != 0)
    {
        p += sprintf(p, "%lu", high_pod[0]);
        p += sprintf(p, "%019lu", mid_pod[0]);
        p += sprintf(p, "%019lu", low_pod[0]);
    }
    else if (mid_pod[0] != 0)
    {
        p += sprintf(p, "%lu", mid_pod[0]);
        p += sprintf(p, "%019lu", low_pod[0]);
    }
    else
    {
        p += sprintf(p, "%lu", low_pod[0]);
    }
    return p - buf;
  }

  //    This method writes unsigned integer representation of TSInt128
  uint8_t TSInt128::writeIntPart(const int128_t& x,
                                 char* buf,
                                 const uint8_t buflen) const
  {
    char* p = buf;
    int128_t high = 0, mid = 0, low = 0;
    uint64_t maxUint64divisor = 10000000000000000000ULL;

    low = x % maxUint64divisor;
    int128_t value = x / maxUint64divisor;
    mid = value % maxUint64divisor;
    high = value / maxUint64divisor;

    p += printPodParts(p, high, mid, low);
    uint8_t written = p - buf;
    if (buflen <= written)
    {
        throw logging::QueryDataExcept("TSInt128::writeIntPart() char buffer overflow.",
                                       logging::formatErr);
    }

    return written;
  }

  //    conversion to std::string
  std::string TSInt128::toString() const
  {
    if (isNullImpl())
    {
      return std::string("NULL");
    }

    if (isEmptyImpl())
    {
      return std::string("EMPTY");
    }

    int128_t tempValue = s128Value;
    char buf[TSInt128::MAXLENGTH16BYTES];
    uint8_t left = sizeof(buf);
    char* p = buf;
    // sign
    if (tempValue < static_cast<int128_t>(0))
    {
      *p++ = '-';
      tempValue *= -1;
      left--; 
    }
    // integer part
    // reduce the size by one to account for \0
    left--;
    p += writeIntPart(tempValue, p, left);
    *p = '\0';

    return std::string(buf);
  }

  // Writes scaled integer part of a TSInt128 using int128 argument provided
  uint8_t TSInt128::writeIntPartWithScale(const int128_t& x,
                                          char* buf,
                                          const uint8_t buflen,
                                          const uint8_t precision,
                                          const int8_t scale) const
  {
    char* p = buf;
    int128_t intPart = x;
    int128_t high = 0, mid = 0, low = 0;
    uint64_t maxUint64divisor = 10000000000000000000ULL;

    // Assuming scale = [0, 56]
    switch (scale / datatypes::maxPowOf10)
    {
      case 2: // scale = [38, 56]
        intPart /= datatypes::mcs_pow_10[datatypes::maxPowOf10];
        intPart /= datatypes::mcs_pow_10[datatypes::maxPowOf10];
        low = intPart;
        break;
      case 1: // scale = [19, 37]
        intPart /= datatypes::mcs_pow_10[datatypes::maxPowOf10];
        intPart /= datatypes::mcs_pow_10[scale % datatypes::maxPowOf10];
        low = intPart % maxUint64divisor;
        mid = intPart / maxUint64divisor;
        break;
      case 0: // scale = [0, 18]
        intPart /= datatypes::mcs_pow_10[scale % datatypes::maxPowOf10];
        low = intPart % maxUint64divisor;
        intPart /= maxUint64divisor;
        mid = intPart % maxUint64divisor;
        high = intPart / maxUint64divisor;
        break;
      default:
        throw logging::QueryDataExcept("TSInt128::writeIntPartWithScale() bad scale",
                                       logging::formatErr);
    }

    p += printPodParts(p, high, mid, low);
    uint8_t written = p - buf;
    if (buflen <= written)
    {
        throw logging::QueryDataExcept("TSInt128::writeIntPartWithScale() char buffer overflow.",
                                       logging::formatErr);
    }

    return written;
  }

  uint8_t TSInt128::writeFractionalPart(const int128_t& x,
                                        char* buf,
                                        const uint8_t buflen,
                                        const uint8_t precision,
                                        const int8_t scale) const
  {
    int128_t scaleDivisor = 1;
    char* p = buf;

    switch (scale / datatypes::maxPowOf10)
    {
      case 2:
        scaleDivisor *= datatypes::mcs_pow_10[datatypes::maxPowOf10];
        scaleDivisor *= datatypes::mcs_pow_10[datatypes::maxPowOf10];
        break;
      case 1:
        scaleDivisor *= datatypes::mcs_pow_10[datatypes::maxPowOf10];
        //fallthrough
      case 0:
        scaleDivisor *= datatypes::mcs_pow_10[scale % datatypes::maxPowOf10];
    }

    int128_t fractionalPart = x % scaleDivisor;

    // divide by the base until we have non-zero quotient
    scaleDivisor /= 10;

    while (scaleDivisor > 1 && fractionalPart / scaleDivisor == 0)
    {
        *p++ = '0';
        scaleDivisor /= 10;
    }
    size_t written = p - buf;;
    p += writeIntPart(fractionalPart, p, buflen - written);
    return p - buf;
  }

  std::string TSInt128::toStringWithScaleImpl(uint8_t precision, int8_t scale) const
  {
    char buf[TSInt128::MAXLENGTH16BYTES];
    uint8_t left = sizeof(buf); 
    char* p = buf;
    int128_t tempValue = s128Value;
    // sign
    if (tempValue < static_cast<int128_t>(0))
    {
        *p++ = '-';
        tempValue *= -1;
        left--;
    }

    // integer part
    p += writeIntPartWithScale(tempValue, p, left, precision, scale);

    // decimal delimiter
    *p++ = '.';
    // decimal part
    left = sizeof(buf) - (p - buf);
    p += writeFractionalPart(tempValue, p, left, precision, scale);

    *p = '\0';

    uint8_t written = p - buf;
    if (sizeof(buf) <= written)
    {
        throw logging::QueryDataExcept("TSInt128::toStringWithScale() char buffer overflow.",
                                       logging::formatErr);
    }
    return std::string(buf);
  }

  std::ostream& operator<<(std::ostream& os, const TSInt128& x)
  {
    os << x.toString();
    return os;
  }

} // end of namespace datatypes
// vim:ts=2 sw=2:
