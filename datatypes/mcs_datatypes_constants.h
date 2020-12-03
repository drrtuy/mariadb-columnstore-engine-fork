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
#ifndef MCS_DATATYPES_CONSTANTS_H_INCLUDED
#define MCS_DATATYPES_CONSTANTS_H_INCLUDED


// A class by Fabio Fernandes pulled off of stackoverflow
// Creates a type _xxl that can be used to create 128bit constant values
// Ex: int128_t i128 = 12345678901234567890123456789_xxl
namespace detail_xxl
{
  using int128_t = __int128;
  using uint128_t = unsigned __int128;

  constexpr uint8_t hexval(char c) 
  { return c>='a' ? (10+c-'a') : c>='A' ? (10+c-'A') : c-'0'; }

  template <int BASE, uint128_t V>
  constexpr uint128_t lit_eval() { return V; }

  template <int BASE, uint128_t V, char C, char... Cs>
  constexpr uint128_t lit_eval() {
    static_assert( BASE!=16 || sizeof...(Cs) <=  32-1, "Literal too large for BASE=16");
    static_assert( BASE!=10 || sizeof...(Cs) <=  39-1, "Literal too large for BASE=10");
    static_assert( BASE!=8  || sizeof...(Cs) <=  44-1, "Literal too large for BASE=8");
    static_assert( BASE!=2  || sizeof...(Cs) <= 128-1, "Literal too large for BASE=2");
    return lit_eval<BASE, BASE*V + hexval(C), Cs...>();
  }

  template<char... Cs > struct LitEval 
  {static constexpr uint128_t eval() {return lit_eval<10,0,Cs...>();} };

  template<char... Cs> struct LitEval<'0','x',Cs...> 
  {static constexpr uint128_t eval() {return lit_eval<16,0,Cs...>();} };

  template<char... Cs> struct LitEval<'0','b',Cs...> 
  {static constexpr uint128_t eval() {return lit_eval<2,0,Cs...>();} };

  template<char... Cs> struct LitEval<'0',Cs...> 
  {static constexpr uint128_t eval() {return lit_eval<8,0,Cs...>();} };

  template<char... Cs> 
  constexpr uint128_t operator "" _xxl() {return LitEval<Cs...>::eval();}
}

namespace datatypes
{

constexpr uint32_t MAXDECIMALWIDTH = 16U;
constexpr uint8_t INT64MAXPRECISION = 18U;
constexpr uint8_t INT128MAXPRECISION = 38U;
constexpr uint8_t MAXLEGACYWIDTH = 8U;
constexpr uint8_t MAXSCALEINC4AVG = 4U;
constexpr int8_t IGNOREPRECISION = -1;

template<char... Cs> 
constexpr uint128_t operator "" _xxl() {return ::detail_xxl::operator "" _xxl<Cs...>();}

const uint64_t mcs_pow_10[20] =
{
  1ULL,
  10ULL,
  100ULL,
  1000ULL,
  10000ULL,
  100000ULL,
  1000000ULL,
  10000000ULL,
  100000000ULL,
  1000000000ULL,
  10000000000ULL,
  100000000000ULL,
  1000000000000ULL,
  10000000000000ULL,
  100000000000000ULL,
  1000000000000000ULL,
  10000000000000000ULL,
  100000000000000000ULL,
  1000000000000000000ULL,
  10000000000000000000ULL,
};
const int128_t mcs_pow_10_128[20] =
{
  10000000000000000000_xxl,
  100000000000000000000_xxl,
  1000000000000000000000_xxl,
  10000000000000000000000_xxl,
  100000000000000000000000_xxl,
  1000000000000000000000000_xxl,
  10000000000000000000000000_xxl,
  100000000000000000000000000_xxl,
  1000000000000000000000000000_xxl,
  10000000000000000000000000000_xxl,
  100000000000000000000000000000_xxl,
  1000000000000000000000000000000_xxl,
  10000000000000000000000000000000_xxl,
  100000000000000000000000000000000_xxl,
  1000000000000000000000000000000000_xxl,
  10000000000000000000000000000000000_xxl,
  100000000000000000000000000000000000_xxl,
  1000000000000000000000000000000000000_xxl,
  10000000000000000000000000000000000000_xxl,
  100000000000000000000000000000000000000_xxl,
};

constexpr uint32_t maxPowOf10 = sizeof(mcs_pow_10)/sizeof(mcs_pow_10[0])-1;
} // end of namespace
#endif // MCS_DATATYPES_CONSTANTS_H_INCLUDED
// vim:ts=2 sw=2:
