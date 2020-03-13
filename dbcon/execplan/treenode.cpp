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

/***********************************************************************
*   $Id: treenode.cpp 9317 2013-03-19 21:37:22Z dhall $
*
*
***********************************************************************/
/** @file */

#include <unistd.h>
#include <string>
#include <exception>
#include <typeinfo>
#include <cstring>

#include "bytestream.h"
#include "treenode.h"
#include "objectreader.h"

using namespace std;
namespace execplan
{

IDB_Decimal::IDB_Decimal(): value(0), scale(0), precision(0)
{
    s128Value = 0;
}

IDB_Decimal::IDB_Decimal(int64_t val, int8_t s, uint8_t p) :
    value (val),
    scale(s),
    precision(p)
{
    s128Value = 0;
}

int IDB_Decimal::decimalComp(const IDB_Decimal& d) const
{
    lldiv_t d1 = lldiv(value, IDB_pow[scale]);
    lldiv_t d2 = lldiv(d.value, IDB_pow[d.scale]);

    int ret = 0;

    if (d1.quot > d2.quot)
    {
        ret = 1;
    }
    else if (d1.quot < d2.quot)
    {
        ret = -1;
    }
    else
    {
        // rem carries the value's sign, but needs to be normalized.
        int64_t s = scale - d.scale;

        if (s < 0)
        {
            if ((d1.rem * IDB_pow[-s]) > d2.rem)
                ret = 1;
            else if ((d1.rem * IDB_pow[-s]) < d2.rem)
                ret = -1;
        }
        else
        {
            if (d1.rem > (d2.rem * IDB_pow[s]))
                ret = 1;
            else if (d1.rem < (d2.rem * IDB_pow[s]))
                ret = -1;
        }
    }

    return ret;
}

bool IDB_Decimal::operator==(const IDB_Decimal& rhs) const
{
    if (utils::widthByPrecision(precision) == 16)
    {
        if (scale == rhs.scale)
            return s128Value == rhs.s128Value;
        else
            return (datatypes::Decimal::compare(*this, rhs) == 0);
    }
    else
    {
        if (scale == rhs.scale)
            return value == rhs.value;
        else
            return (decimalComp(rhs) == 0);
    }
}

bool IDB_Decimal::operator>(const IDB_Decimal& rhs) const
{
    if (utils::widthByPrecision(precision) == 16)
    {
        if (scale == rhs.scale)
            return s128Value > rhs.s128Value;
        else
            return (datatypes::Decimal::compare(*this, rhs) > 0);
    }
    else
    {
        if (scale == rhs.scale)
            return value > rhs.value;
        else
            return (decimalComp(rhs) > 0);
    }
}

bool IDB_Decimal::operator<(const IDB_Decimal& rhs) const
{
    if (utils::widthByPrecision(precision) == 16)
    {
        if (scale == rhs.scale)
            return s128Value < rhs.s128Value;
        else
            return (datatypes::Decimal::compare(*this, rhs) < 0);
    }
    else
    {
        if (scale == rhs.scale)
            return value < rhs.value;
        else
            return (decimalComp(rhs) < 0);
    }
}

bool IDB_Decimal::operator>=(const IDB_Decimal& rhs) const
{
    if (utils::widthByPrecision(precision) == 16)
    {
        if (scale == rhs.scale)
            return s128Value >= rhs.s128Value;
        else
            return (datatypes::Decimal::compare(*this, rhs) >= 0);
    }
    else
    {
        if (scale == rhs.scale)
            return value >= rhs.value;
        else
            return (decimalComp(rhs) >= 0);
    }
}

bool IDB_Decimal::operator<=(const IDB_Decimal& rhs) const
{
    if (utils::widthByPrecision(precision) == 16)
    {
        if (scale == rhs.scale)
            return s128Value <= rhs.s128Value;
        else
            return (datatypes::Decimal::compare(*this, rhs) <= 0);
    }
    else
    {
        if (scale == rhs.scale)
            return value <= rhs.value;
        else
            return (decimalComp(rhs) <= 0);
    }
}

bool IDB_Decimal::operator!=(const IDB_Decimal& rhs) const
{
    if (utils::widthByPrecision(precision) == 16)
    {
        if (scale == rhs.scale)
            return s128Value != rhs.s128Value;
        else
            return (datatypes::Decimal::compare(*this, rhs) != 0);
    }
    else
    {
        if (scale == rhs.scale)
            return value != rhs.value;
        else
            return (decimalComp(rhs) != 0);
    }
}

/**
 * Constructors/Destructors
 */
TreeNode::TreeNode(): fDerivedTable(""),
    fRefCount(0),
    fDerivedRefCol(NULL)
{
    memset(tmp, 0, 312);
}

TreeNode::TreeNode(const TreeNode& rhs):
    fResult(rhs.fResult),
    fResultType(rhs.resultType()),
    fOperationType(rhs.operationType()),
    fRegex (rhs.regex()),
    fDerivedTable (rhs.derivedTable()),
    fRefCount(rhs.refCount()),
    fDerivedRefCol(rhs.derivedRefCol())
{
    memcpy(tmp, rhs.tmp, 312);
}

TreeNode::~TreeNode() {}

void TreeNode::resultType ( const execplan::CalpontSystemCatalog::ColType& resultType)
{
    fResultType = resultType;

    // set scale/precision for the result
    if (fResultType.colDataType == execplan::CalpontSystemCatalog::DECIMAL ||
            fResultType.colDataType == execplan::CalpontSystemCatalog::UDECIMAL)
    {
        fResult.decimalVal.scale = fResultType.scale;
        fResult.decimalVal.precision = fResultType.precision;
    }
}

/**
 * ostream function
 */
ostream& operator<<(ostream& output, const TreeNode& rhs)
{
    output << rhs.toString();
    return output;
}

}  /* namespace */
