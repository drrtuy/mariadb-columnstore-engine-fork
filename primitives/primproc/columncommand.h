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

//
// $Id: columncommand.h 2057 2013-02-13 17:00:10Z pleblanc $
// C++ Interface: columncommand
//
// Description:
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#ifndef COLUMNCOMMAND_H_
#define COLUMNCOMMAND_H_

#include "command.h"
#include "calpontsystemcatalog.h"


namespace primitiveprocessor
{

// #warning got the ColumnCommand definition
class ColumnCommand : public Command
{
public:
    ColumnCommand();
    ColumnCommand(execplan::ColumnCommandDataType& aColType);
    virtual ~ColumnCommand();

    inline uint64_t getLBID()
    {
        return lbid;
    }
    inline uint8_t getWidth()
    {
        return colType.colWidth;
    }
    inline uint8_t getScale()
    {
        return colType.scale;
    }
    uint16_t getFilterCount()
    {
        return filterCount;
    }
    const execplan::ColumnCommandDataType & getColType()
    {
        return colType;
    }

    void execute();
    void execute(int64_t* vals);	//used by RTSCommand to redirect values
    virtual void prep(int8_t outputType, bool absRids);
    void project();
    void projectIntoRowGroup(rowgroup::RowGroup& rg, uint32_t pos);
    void nextLBID();
    bool isScan()
    {
        return _isScan;
    }
    void createCommand(messageqcpp::ByteStream&); // obsolete method
    void createCommand(execplan::ColumnCommandDataType& aColType, messageqcpp::ByteStream&);
    void resetCommand(messageqcpp::ByteStream&);
    void setMakeAbsRids(bool m)
    {
        makeAbsRids = m;
    }
    bool willPrefetch();
    int64_t getLastLbid();
    void getLBIDList(uint32_t loopCount, std::vector<int64_t>* lbids);

    virtual SCommand duplicate();
    bool operator==(const ColumnCommand&) const;
    bool operator!=(const ColumnCommand&) const;

    /* OR hacks */
    void setScan(bool b)
    {
        _isScan = b;
    }
    void disableFilters();
    void enableFilters();

    int getCompType() const
    {
        return colType.compressionType;
    }

protected:
    virtual void loadData();
    template <int W>
    void _loadDataTemplate();
    void updateCPDataNarrow();
    void updateCPDataWide();
    void _issuePrimitive();
    void duplicate(ColumnCommand*);
    void fillInPrimitiveMessageHeader(const int8_t outputType, const bool absRids);

    // we only care about the width and type fields.
    //On the PM the rest is uninitialized
    execplan::ColumnCommandDataType colType;

    ColumnCommand(const ColumnCommand&);
    ColumnCommand& operator=(const ColumnCommand&);

    void _execute();
    void issuePrimitive();
    void processResult();
    template<int W>
    void _process_OT_BOTH();
    template<int W>
    void _process_OT_BOTH_wAbsRids();
    virtual void process_OT_BOTH();
    void process_OT_RID();
    void process_OT_DATAVALUE();
    void process_OT_ROWGROUP();
    void projectResult();
    void projectResultRG(rowgroup::RowGroup& rg, uint32_t pos);
    void removeRowsFromRowGroup(rowgroup::RowGroup&);
    void makeScanMsg();
    void makeStepMsg();
    void setLBID(uint64_t rid);
    template<typename T>
    inline void fillEmptyBlock(uint8_t* dst,
                        const uint8_t*emptyValue,
                        const uint32_t number) const;

    bool _isScan;

    boost::scoped_array<uint8_t> inputMsg;
    NewColRequestHeader* primMsg;
    NewColResultHeader* outMsg;

    // the length of base prim msg, which is everything up to the
    // rid array for the pCol message
    uint32_t baseMsgLength;

    uint64_t lbid;
    uint32_t traceFlags;  // probably move this to Command
    uint8_t BOP;
    messageqcpp::ByteStream filterString;
    uint16_t filterCount;
    bool makeAbsRids;
    int64_t* values;      // this is usually bpp->values; RTSCommand needs to use a different container
    int128_t* wide128Values;

    uint16_t mask, shift;  // vars for the selective block loader

    // counters to decide whether to prefetch or not
    uint32_t blockCount, loadCount;

    boost::shared_ptr<primitives::ParsedColumnFilter> parsedColumnFilter;

    /* OR hacks */
    boost::shared_ptr<primitives::ParsedColumnFilter> emptyFilter;
    bool suppressFilter;

    std::vector<uint64_t> lastLbid;

    /* speculative optimizations for projectintorowgroup() */
    rowgroup::Row r;
    uint32_t rowSize;

    bool wasVersioned;

    friend class RTSCommand;
};

using ColumnCommandShPtr = std::unique_ptr<ColumnCommand>;

class ColumnCommandInt8 : public ColumnCommand
{
  public:
    static constexpr uint8_t size = 1;
    using ColumnCommand::ColumnCommand;
    ColumnCommandInt8(execplan::ColumnCommandDataType& colType, messageqcpp::ByteStream& bs);
    void prep(int8_t outputType, bool absRids) override;
    void loadData() override;
    void process_OT_BOTH() override;
};

class ColumnCommandInt16 : public ColumnCommand
{
  public:
    static constexpr uint8_t size = 2;
    using ColumnCommand::ColumnCommand;
    ColumnCommandInt16(execplan::ColumnCommandDataType& colType, messageqcpp::ByteStream& bs);
    void prep(int8_t outputType, bool absRids) override;
    void loadData() override;
    void process_OT_BOTH() override;
};

class ColumnCommandInt32 : public ColumnCommand
{
  public:
    static constexpr uint8_t size = 4;
    using ColumnCommand::ColumnCommand;
    ColumnCommandInt32(execplan::ColumnCommandDataType& colType, messageqcpp::ByteStream& bs);
    void prep(int8_t outputType, bool absRids) override;
    void loadData() override;
    void process_OT_BOTH() override;
};

class ColumnCommandInt64 : public ColumnCommand
{
  public:
    static constexpr uint8_t size = 8;
    using ColumnCommand::ColumnCommand;
    ColumnCommandInt64(execplan::ColumnCommandDataType& colType, messageqcpp::ByteStream& bs);
    void prep(int8_t outputType, bool absRids) override;
    void loadData() override;
    void process_OT_BOTH() override;
};

class ColumnCommandInt128 : public ColumnCommand
{
  public:
    static constexpr uint8_t size = 16;
    using ColumnCommand::ColumnCommand;
    ColumnCommandInt128(execplan::ColumnCommandDataType& colType, messageqcpp::ByteStream& bs);
    void prep(int8_t outputType, bool absRids) override;
    void loadData() override;
    void process_OT_BOTH() override;
};


class ColumnCommandFabric
{
  public:
    ColumnCommandFabric() = default;
    static ColumnCommand* createCommand(messageqcpp::ByteStream& bs);
    static ColumnCommand* duplicate(const ColumnCommandShPtr& rhs);
};

template<typename T>
inline void ColumnCommand::fillEmptyBlock(uint8_t* dst,
                                   const uint8_t*emptyValue,
                                   const uint32_t number) const
{
    T* typedDst = reinterpret_cast<T*>(dst);
    const T* typedEmptyValue = reinterpret_cast<const T*>(emptyValue);
    for (uint32_t idx = 0; idx < number; idx = idx + 4)
    {
        typedDst[idx] = *typedEmptyValue;
        typedDst[idx+1] = *typedEmptyValue;
        typedDst[idx+2] = *typedEmptyValue;
        typedDst[idx+3] = *typedEmptyValue;
    }
}

template<>
inline void ColumnCommand::fillEmptyBlock<messageqcpp::ByteStream::hexbyte>(uint8_t* dst,
                                                               const uint8_t*emptyValue,
                                                               const uint32_t number) const
{
    for (uint32_t idx = 0; idx < number; idx++)
    {
        datatypes::TSInt128::assignPtrPtr(dst + idx * sizeof(messageqcpp::ByteStream::hexbyte),
                                          emptyValue);
    }
}

} // namespace

#endif
// vim:ts=4 sw=4:

