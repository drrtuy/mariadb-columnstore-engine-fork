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
// $Id: columncommand.cpp 2057 2013-02-13 17:00:10Z pleblanc $
// C++ Implementation: columncommand
//
// Description:
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
// Copyright: See COPYING file that comes with this distribution
//
//

#include <unistd.h>
#include <sstream>
#include <map>
#include <cstdlib>
#include <cmath>
using namespace std;

#include "bpp.h"
#include "errorcodes.h"
#include "exceptclasses.h"
#include "primitiveserver.h"
#include "primproc.h"
#include "stats.h"
#include "datatypes/mcs_int128.h"

using namespace messageqcpp;
using namespace rowgroup;

#include "messageids.h"
using namespace logging;

#ifdef _MSC_VER
#define llabs labs
#endif

namespace primitiveprocessor
{

extern int noVB;

ColumnCommand::ColumnCommand() :
    Command(COLUMN_COMMAND),
    blockCount(0),
    loadCount(0),
    suppressFilter(false)
{
}

ColumnCommand::ColumnCommand(execplan::ColumnCommandDataType& aColType)
: ColumnCommand()
{
    colType = aColType;
}

ColumnCommand::~ColumnCommand() { }

void ColumnCommand::_execute()
{
    if (_isScan)
        makeScanMsg();
    else if (bpp->ridCount == 0)     // this would cause a scan
    {
        blockCount += colType.colWidth;
        return;  // a step with no input rids does nothing
    }
    else
        makeStepMsg();

    issuePrimitive();
    processResult();

    // check if feeding a filtercommand
    if (fFilterFeeder != NOT_FEEDER)
        copyRidsForFilterCmd();
}

void ColumnCommand::execute()
{
    if (fFilterFeeder == LEFT_FEEDER)
    {
        values = bpp->fFiltCmdValues[0].get();
        wide128Values = bpp->fFiltCmdBinaryValues[0].get();
    }
    else if (fFilterFeeder == RIGHT_FEEDER)
    {
        values = bpp->fFiltCmdValues[1].get();
        wide128Values = bpp->fFiltCmdBinaryValues[1].get();
    }
    else
    {
        values = bpp->values;
        wide128Values = bpp->wide128Values;
    }

    _execute();
}

void ColumnCommand::execute(int64_t* vals)
{
    values = vals;
    _execute();
}

void ColumnCommand::makeScanMsg()
{
    /* Finish the NewColRequestHeader. */

    /* XXXPAT: if there is a Command preceeding this one, it's a DictScan feeding tokens
    which need to become filter elements.  Can we handle that with this design?
    Implement that later. */

    primMsg->ism.Size = baseMsgLength;
    primMsg->NVALS = 0;
    primMsg->LBID = lbid;
    primMsg->RidFlags = 0xFFFF;

// 	cout << "scanning lbid " << lbid << " colwidth = " << primMsg->DataSize <<
// 		" filterCount = " << filterCount << " outputType = " <<
// 		(int) primMsg->OutputType << endl;
}

void ColumnCommand::makeStepMsg()
{
    memcpy(&inputMsg[baseMsgLength], bpp->relRids, bpp->ridCount << 1);
    primMsg->RidFlags = bpp->ridMap;
    primMsg->ism.Size = baseMsgLength +  (bpp->ridCount << 1);
    primMsg->NVALS = bpp->ridCount;
    primMsg->LBID = lbid;
// 	cout << "lbid is " << lbid << endl;
}

void ColumnCommand::loadData()
{
    uint32_t wasCached;
    uint32_t blocksRead;
    uint16_t _mask;
    uint64_t oidLastLbid = 0;
    bool lastBlockReached = false;
    oidLastLbid = getLastLbid();
    uint32_t blocksToLoad = 0;
    // The number of elements allocated equals to the number of
    // iteratations of the first loop here.
    BRM::LBID_t* lbids = (BRM::LBID_t*) alloca(colType.colWidth * sizeof(BRM::LBID_t));
    uint8_t** blockPtrs = (uint8_t**) alloca(colType.colWidth * sizeof(uint8_t*));
    int i;


    _mask = mask;
// 	primMsg->RidFlags = 0xffff;   // disables selective block loading
    //cout <<__FILE__ << "::issuePrimitive() o: " << getOID() << " l:" << primMsg->LBID << " ll: " << oidLastLbid << endl;

    for (i = 0; i < colType.colWidth; ++i, _mask <<= shift)
    {

        if ((!lastBlockReached && _isScan) || (!_isScan && primMsg->RidFlags & _mask))
        {
            lbids[blocksToLoad] = primMsg->LBID + i;
            blockPtrs[blocksToLoad] = &bpp->blockData[i * BLOCK_SIZE];
            blocksToLoad++;
            loadCount++;
        }
        else if (lastBlockReached && _isScan)
        {
            // fill remaining blocks with empty values when col scan
            uint32_t blockLen = BLOCK_SIZE / colType.colWidth;
            auto attrs = datatypes::SystemCatalog::TypeAttributesStd(colType.colWidth,
                                                                     0,
                                                                     -1);
            const auto* typeHandler = datatypes::TypeHandler::find(colType.colDataType,
                                                                   attrs);
            const uint8_t* emptyValue = typeHandler->getEmptyValueForType(attrs);
            uint8_t* blockDataPtr = &bpp->blockData[i * BLOCK_SIZE];

            idbassert(blockDataPtr);
            if (colType.colWidth == sizeof(ByteStream::byte))
            {
                fillEmptyBlock<ByteStream::byte>(blockDataPtr, emptyValue, blockLen);
            }
            if (colType.colWidth == sizeof(ByteStream::doublebyte))
            {
                fillEmptyBlock<ByteStream::doublebyte>(blockDataPtr, emptyValue, blockLen);
            }
            if (colType.colWidth == sizeof(ByteStream::quadbyte))
            {
                fillEmptyBlock<ByteStream::quadbyte>(blockDataPtr, emptyValue, blockLen);
            }
            if (colType.colWidth == sizeof(ByteStream::octbyte))
            {
                fillEmptyBlock<ByteStream::octbyte>(blockDataPtr, emptyValue, blockLen);
            }
            if (colType.colWidth == sizeof(ByteStream::hexbyte))
            {
                fillEmptyBlock<ByteStream::hexbyte>(blockDataPtr, emptyValue, blockLen);
            }
        }// else

        if ( (primMsg->LBID + i) == oidLastLbid)
            lastBlockReached = true;

        blockCount++;
    } // for

    /* Do the load */
    wasCached = primitiveprocessor::loadBlocks(lbids,
                bpp->versionInfo,
                bpp->txnID,
                colType.compressionType,
                blockPtrs,
                &blocksRead,
                bpp->LBIDTrace,
                bpp->sessionID,
                blocksToLoad,
                &wasVersioned,
                willPrefetch(),
                &bpp->vssCache);
    bpp->cachedIO += wasCached;
    bpp->physIO += blocksRead;
    bpp->touchedBlocks += blocksToLoad;
}

void ColumnCommand::issuePrimitive()
{
    uint32_t resultSize;

    loadData();

    if (!suppressFilter)
        bpp->pp.setParsedColumnFilter(parsedColumnFilter);
    else
        bpp->pp.setParsedColumnFilter(emptyFilter);

    bpp->pp.p_Col(primMsg, outMsg, bpp->outMsgSize, (unsigned int*)&resultSize);

    /* Update CP data, the PseudoColumn code should always be !_isScan.  Should be safe
        to leave this here for now. */
    if (_isScan)
    {
        bpp->validCPData = (outMsg->ValidMinMax && !wasVersioned);
        //if (wasVersioned && outMsg->ValidMinMax)
        //	cout << "CC: versioning overriding min max data\n";
        bpp->lbidForCP = lbid;
        if (UNLIKELY(utils::isWide(colType.colWidth)))
        {
            if (colType.isWideDecimalType())
            {
                bpp->hasWideColumnOut = true;
                // colWidth is int32 and wideColumnWidthOut's
                // value is expected to be at most uint8.
                bpp->wideColumnWidthOut = colType.colWidth;
                bpp->max128Val = outMsg->Max;
                bpp->min128Val = outMsg->Min;
            }
            else
            {
                ostringstream os;
                os << " WARNING!!! Not implemented for ";
                os << primMsg->colType.DataSize << " column.";
                throw PrimitiveColumnProjectResultExcept(os.str());
            }
        }
        else
        {
            bpp->maxVal = static_cast<int64_t>(outMsg->Max);
            bpp->minVal = static_cast<int64_t>(outMsg->Min);
        }
    }

} // issuePrimitive()

void ColumnCommand::process_OT_BOTH()
{
    uint64_t i, pos;

    bpp->ridCount = outMsg->NVALS;
    bpp->ridMap = outMsg->RidFlags;

    /* this is verbose and repetative to minimize the work per row */
    switch (colType.colWidth)
    {
        case 16:
            for (i = 0, pos = sizeof(NewColResultHeader); i < outMsg->NVALS; ++i)
            {
                if (makeAbsRids)
                    bpp->absRids[i] = *((uint16_t*) &bpp->outputMsg[pos]) + bpp->baseRid;

                bpp->relRids[i] = *((uint16_t*) &bpp->outputMsg[pos]);
                pos += 2;
                datatypes::TSInt128::assignPtrPtr(&wide128Values[i], &bpp->outputMsg[pos]);
                pos += 16;
            }

            break;

        case 8:
            for (i = 0, pos = sizeof(NewColResultHeader); i < outMsg->NVALS; ++i)
            {
                if (makeAbsRids)
                    bpp->absRids[i] = *((uint16_t*) &bpp->outputMsg[pos]) + bpp->baseRid;

                bpp->relRids[i] = *((uint16_t*) &bpp->outputMsg[pos]);
                pos += 2;
                values[i] = *((int64_t*) &bpp->outputMsg[pos]);
                pos += 8;
            }

            break;

        case 4:
            for (i = 0, pos = sizeof(NewColResultHeader); i < outMsg->NVALS; ++i)
            {
                if (makeAbsRids)
                    bpp->absRids[i] = *((uint16_t*) &bpp->outputMsg[pos]) + bpp->baseRid;

                bpp->relRids[i] = *((uint16_t*) &bpp->outputMsg[pos]);
                pos += 2;
                values[i] = *((int32_t*) &bpp->outputMsg[pos]);
                pos += 4;
            }

            break;

        case 2:
            for (i = 0, pos = sizeof(NewColResultHeader); i < outMsg->NVALS; ++i)
            {
                if (makeAbsRids)
                    bpp->absRids[i] = *((uint16_t*) &bpp->outputMsg[pos]) + bpp->baseRid;

                bpp->relRids[i] = *((uint16_t*) &bpp->outputMsg[pos]);
                pos += 2;
                values[i] = *((int16_t*) &bpp->outputMsg[pos]);
                pos += 2;
            }

            break;

        case 1:
            for (i = 0, pos = sizeof(NewColResultHeader); i < outMsg->NVALS; ++i)
            {
                if (makeAbsRids)
                    bpp->absRids[i] = *((uint16_t*) &bpp->outputMsg[pos]) + bpp->baseRid;

                bpp->relRids[i] = *((uint16_t*) &bpp->outputMsg[pos]);
                pos += 2;
                values[i] = *((int8_t*) &bpp->outputMsg[pos++]);
            }

            break;
    }

}

void ColumnCommand::process_OT_RID()
{
    memcpy(bpp->relRids, outMsg + 1, outMsg->NVALS << 1);
    bpp->ridCount = outMsg->NVALS;
    bpp->ridMap = outMsg->RidFlags;
}

void ColumnCommand::process_OT_DATAVALUE()
{
    bpp->ridCount = outMsg->NVALS;

    switch (colType.colWidth)
    {
         case 16:
         {
            memcpy(wide128Values, outMsg + 1, outMsg->NVALS << 4);
            break;
         }

        case 8:
        {
            memcpy(values, outMsg + 1, outMsg->NVALS << 3);
            break;
        }

        case 4:
        {
            int32_t* arr32 = (int32_t*) (outMsg + 1);

            for (uint64_t i = 0; i < outMsg->NVALS; ++i)
                values[i] = arr32[i];

            break;
        }

        case 2:
        {
            int16_t* arr16 = (int16_t*) (outMsg + 1);

            for (uint64_t i = 0; i < outMsg->NVALS; ++i)
                values[i] = arr16[i];

            break;
        }

        case 1:
        {
            int8_t* arr8 = (int8_t*) (outMsg + 1);

            for (uint64_t i = 0; i < outMsg->NVALS; ++i)
                values[i] = arr8[i];

            break;
        }
    }
}

void ColumnCommand::processResult()
{
    /* Switch on output type, turn pCol output into something useful, store it in
       the containing BPP */

// 	if (filterCount == 0 && !_isScan)
// 		idbassert(outMsg->NVALS == bpp->ridCount);

    switch (outMsg->OutputType)
    {
        case OT_BOTH:
            process_OT_BOTH();
            break;

        case OT_RID:
            process_OT_RID();
            break;

        case OT_DATAVALUE:
            process_OT_DATAVALUE();
            break;

        default:
            cout << "outputType = " << outMsg->OutputType << endl;
            throw logic_error("ColumnCommand got a bad OutputType");
    }

    // check if feeding a filtercommand
    if (fFilterFeeder == LEFT_FEEDER)
    {
        bpp->fFiltRidCount[0] = bpp->ridCount;

        for (uint64_t i = 0; i < bpp->ridCount; i++)
            bpp->fFiltCmdRids[0][i] = bpp->relRids[i];
    }
    else if (fFilterFeeder == RIGHT_FEEDER)
    {
        bpp->fFiltRidCount[1] = bpp->ridCount;

        for (uint64_t i = 0; i < bpp->ridCount; i++)
            bpp->fFiltCmdRids[1][i] = bpp->relRids[i];
    }
}

void ColumnCommand::createCommand(ByteStream& bs)
{
    throw runtime_error("Obsolete method that must not be used");
}

void ColumnCommand::createCommand(execplan::ColumnCommandDataType& aColType, ByteStream& bs)
{
    colType = aColType;
    uint8_t tmp8;

    bs >> tmp8;
    _isScan = tmp8;
    bs >> traceFlags;
    bs >> filterString;
    bs >> BOP;
    bs >> filterCount;
    deserializeInlineVector(bs, lastLbid);

    Command::createCommand(bs);
}

void ColumnCommand::resetCommand(ByteStream& bs)
{
    bs >> lbid;
}

void ColumnCommand::prep(int8_t outputType, bool absRids)
{
    throw std::runtime_error("ColumnCommand::prep(): Obsolete function that can not be used.");
}

void ColumnCommand::_prep(int8_t outputType, bool absRids)
{
    /* make the template NewColRequestHeader */

    baseMsgLength = sizeof(NewColRequestHeader) +
                    (suppressFilter ? 0 : filterString.length());

    if (!inputMsg)
        inputMsg.reset(new uint8_t[baseMsgLength + (LOGICAL_BLOCK_RIDS * 2)]);

    primMsg = (NewColRequestHeader*) inputMsg.get();
    outMsg = (NewColResultHeader*) bpp->outputMsg.get();
    makeAbsRids = absRids;

    primMsg->ism.Interleave = 0;
    primMsg->ism.Flags = 0;
// 	primMsg->ism.Flags = PrimitiveMsg::planFlagsToPrimFlags(traceFlags);
    primMsg->ism.Command = COL_BY_SCAN;
    primMsg->ism.Size = sizeof(NewColRequestHeader) + (suppressFilter ? 0 : filterString.length());
    primMsg->ism.Type = 2;
    primMsg->hdr.SessionID = bpp->sessionID;
    //primMsg->hdr.StatementID = 0;
    primMsg->hdr.TransactionID = bpp->txnID;
    primMsg->hdr.VerID = bpp->versionInfo.currentScn;
    primMsg->hdr.StepID = bpp->stepID;
    primMsg->colType = ColRequestHeaderDataType(colType);
    primMsg->OutputType = outputType;
    primMsg->BOP = BOP;
    primMsg->NOPS = (suppressFilter ? 0 : filterCount);
    primMsg->sort = 0;
/*
    switch (colType.colWidth)
    {
        case 1:
            shift = 16;
            mask = 0xFFFF;
            break;

        case 2:
            shift = 8;
            mask = 0xFF;
            break;

        case 4:
            shift = 4;
            mask = 0x0F;
            break;

        case 8:
            shift = 2;
            mask = 0x03;
            break;

        case 16:
            shift = 1;
            mask = 0x01;
            break;
        default:
            cout << "CC: colWidth is " << colType.colWidth << endl;
            throw logic_error("ColumnCommand: bad column width?");
    }
*/
}

/* Assumes OT_DATAVALUE */
void ColumnCommand::projectResult()
{
    if (primMsg->NVALS != outMsg->NVALS || outMsg->NVALS != bpp->ridCount )
    {
        ostringstream os;
        BRM::DBRM brm;
        BRM::OID_t oid;
        uint16_t l_dbroot;
        uint32_t partNum;
        uint16_t segNum;
        uint32_t fbo;
        brm.lookupLocal(lbid, 0, false, oid, l_dbroot, partNum, segNum, fbo);

        os << __FILE__ << " error on projection for oid " << oid << " lbid " << lbid;

        if (primMsg->NVALS != outMsg->NVALS )
            os << ": input rids " << primMsg->NVALS;
        else
            os << ": ridcount " << bpp->ridCount;

        os << ", output rids " << outMsg->NVALS << endl;

        //cout << os.str();
        if (bpp->sessionID & 0x80000000)
            throw NeedToRestartJob(os.str());
        else
            throw PrimitiveColumnProjectResultExcept(os.str());
    }

    idbassert(primMsg->NVALS == outMsg->NVALS);
    idbassert(outMsg->NVALS == bpp->ridCount);
    *bpp->serialized << (uint32_t) (outMsg->NVALS * colType.colWidth);
    bpp->serialized->append((uint8_t*) (outMsg + 1), outMsg->NVALS * colType.colWidth);
}

void ColumnCommand::removeRowsFromRowGroup(RowGroup& rg)
{
    uint32_t gapSize = colType.colWidth + 2;
    uint8_t* msg8;
    uint16_t rid;
    Row oldRow, newRow;
    uint32_t oldIdx, newIdx;

    rg.initRow(&oldRow);
    rg.initRow(&newRow);
    rg.getRow(0, &oldRow);
    rg.getRow(0, &newRow);
    msg8 = (uint8_t*) (outMsg + 1);

    for (oldIdx = newIdx = 0; newIdx < outMsg->NVALS; newIdx++, msg8 += gapSize)
    {
        rid = *((uint16_t*) msg8);

        while (rid != bpp->relRids[oldIdx])
        {
            // need to find rid in relrids, and it is in there
            oldIdx++;
            oldRow.nextRow();
        }

        if (oldIdx != newIdx)
        {
            bpp->relRids[newIdx] = rid;
            // we use a memcpy here instead of copyRow() to avoid expanding the string table;
            memcpy(newRow.getData(), oldRow.getData(), newRow.getSize());
        }

        oldIdx++;
        oldRow.nextRow();
        newRow.nextRow();
    }

    rg.setRowCount(outMsg->NVALS);   // this gets rid of trailing rows, no need to set them to NULL
    bpp->ridCount = outMsg->NVALS;
    primMsg->NVALS = outMsg->NVALS;
}

void ColumnCommand::projectResultRG(RowGroup& rg, uint32_t pos)
{
    uint32_t i, offset, gapSize;
    uint8_t* msg8 = (uint8_t*) (outMsg + 1);

    if (noVB)
    {
        // outMsg has rids in this case
        msg8 += 2;
        gapSize = colType.colWidth + 2;
    }
    else
        gapSize = colType.colWidth;

    /* TODO: reoptimize these away */
    rg.initRow(&r);
    offset = r.getOffset(pos);
    rowSize = r.getSize();

    if ((primMsg->NVALS != outMsg->NVALS || outMsg->NVALS != bpp->ridCount) && (!noVB || bpp->sessionID & 0x80000000))
    {
        ostringstream os;
        BRM::DBRM brm;
        BRM::OID_t oid;
        uint16_t dbroot;
        uint32_t partNum;
        uint16_t segNum;
        uint32_t fbo;
        brm.lookupLocal(lbid, 0, false, oid, dbroot, partNum, segNum, fbo);

        os << __FILE__ << " error on projectResultRG for oid " << oid << " lbid " << lbid;

        if (primMsg->NVALS != outMsg->NVALS )
            os << ": input rids " << primMsg->NVALS;
        else
            os << ": ridcount " << bpp->ridCount;

        os << ",  output rids " << outMsg->NVALS;
        /*
        		BRM::VSSData entry;
        		if (bpp->vssCache.find(lbid) != bpp->vssCache.end()) {
        			entry = bpp->vssCache[lbid];
        			if (entry.returnCode == 0)
        				os << "  requested version " << entry.verID;
        		}
        */
        os << endl;

        if (bpp->sessionID & 0x80000000)
            throw NeedToRestartJob(os.str());
        else
            throw PrimitiveColumnProjectResultExcept(os.str());
    }
    else if (primMsg->NVALS != outMsg->NVALS || outMsg->NVALS != bpp->ridCount)
        removeRowsFromRowGroup(rg);

    idbassert(primMsg->NVALS == outMsg->NVALS);
    idbassert(outMsg->NVALS == bpp->ridCount);
    rg.getRow(0, &r);

    switch (colType.colWidth)
    {
        case 1:
        {
            for (i = 0; i < outMsg->NVALS; ++i, msg8 += gapSize)
            {
                r.setUintField_offset<1>(*msg8, offset);
                r.nextRow(rowSize);
            }

            break;
        }

        case 2:
        {
            for (i = 0; i < outMsg->NVALS; ++i, msg8 += gapSize)
            {
                r.setUintField_offset<2>(*((uint16_t*) msg8), offset);
                r.nextRow(rowSize);
            }

            break;
        }

        case 4:
        {
            for (i = 0; i < outMsg->NVALS; ++i, msg8 += gapSize)
            {
                r.setUintField_offset<4>(*((uint32_t*) msg8), offset);
                r.nextRow(rowSize);
            }
            break;
        }
        case 8:
        {
            for (i = 0; i < outMsg->NVALS; ++i, msg8 += gapSize)
            {
                r.setUintField_offset<8>(*((uint64_t*) msg8), offset);
                r.nextRow(rowSize);
            }
            break;
        }
        case 16:
        {
            for (i = 0; i < outMsg->NVALS; ++i, msg8 += gapSize)
            {
                r.setBinaryField_offset((int128_t*)msg8, colType.colWidth, offset);
                r.nextRow(rowSize);
            }
            break;
        }

    }
}

void ColumnCommand::project()
{
    /* bpp->ridCount == 0 would signify a scan operation */
    if (bpp->ridCount == 0)
    {
        *bpp->serialized << (uint32_t) 0;
        blockCount += colType.colWidth;
        return;
    }

    makeStepMsg();
    issuePrimitive();
    projectResult();
}

void ColumnCommand::projectIntoRowGroup(RowGroup& rg, uint32_t pos)
{
    if (bpp->ridCount == 0)
    {
        blockCount += colType.colWidth;
        return;
    }

    makeStepMsg();
    issuePrimitive();
    projectResultRG(rg, pos);
}

void ColumnCommand::nextLBID()
{
    lbid += colType.colWidth;
}

void ColumnCommand::duplicate(ColumnCommand* cc)
{
    cc->_isScan = _isScan;
    cc->traceFlags = traceFlags;
    cc->filterString = filterString;
    cc->colType = colType;
    cc->BOP = BOP;
    cc->filterCount = filterCount;
    cc->fFilterFeeder = fFilterFeeder;
    cc->parsedColumnFilter = parsedColumnFilter;
    cc->suppressFilter = suppressFilter;
    cc->lastLbid = lastLbid;
    cc->r = r;
    cc->rowSize = rowSize;
    cc->Command::duplicate(this);
}

SCommand ColumnCommand::duplicate()
{
/*
    SCommand ret;

    ret.reset(new ColumnCommand());
    duplicate((ColumnCommand*) ret.get());
    return ret;
*/
    return SCommand(ColumnCommandFabric::duplicate(this));
}

bool ColumnCommand::operator==(const ColumnCommand& cc) const
{
    if (_isScan != cc._isScan)
        return false;

    if (BOP != cc.BOP)
        return false;

    if (filterString != cc.filterString)
        return false;

    if (filterCount != cc.filterCount)
        return false;

    if (makeAbsRids != cc.makeAbsRids)
        return false;

    if (colType.colWidth != cc.colType.colWidth)
        return false;

    if (colType.colDataType != cc.colType.colDataType)
        return false;

    return true;
}

bool ColumnCommand::operator!=(const ColumnCommand& cc) const
{
    return !(*this == cc);
}


ColumnCommand& ColumnCommand::operator=(const ColumnCommand& c)
{
    _isScan = c._isScan;
    traceFlags = c.traceFlags;
    filterString = c.filterString;
    colType = c.colType;
    BOP = c.BOP;
    filterCount = c.filterCount;
    fFilterFeeder = c.fFilterFeeder;
    parsedColumnFilter = c.parsedColumnFilter;
    suppressFilter = c.suppressFilter;
    lastLbid = c.lastLbid;
    return *this;
}

bool ColumnCommand::willPrefetch()
{
// 	if (blockCount > 0)
// 		cout << "load rate = " << ((double)loadCount)/((double)blockCount) << endl;

//	if (!((double)loadCount)/((double)blockCount) > bpp->prefetchThreshold)
//		cout << "suppressing prefetch\n";

    //return false;
    return (blockCount == 0 || ((double)loadCount) / ((double)blockCount) >
            bpp->prefetchThreshold);
}

void ColumnCommand::disableFilters()
{
    suppressFilter = true;
    prep(primMsg->OutputType, makeAbsRids);
}

void ColumnCommand::enableFilters()
{
    suppressFilter = false;
    prep(primMsg->OutputType, makeAbsRids);
}

void ColumnCommand::getLBIDList(uint32_t loopCount, vector<int64_t>* lbids)
{
    int64_t firstLBID = lbid, lastLBID = firstLBID + (loopCount * colType.colWidth) - 1, i;

    for (i = firstLBID; i <= lastLBID; i++)
        lbids->push_back(i);
}

int64_t ColumnCommand::getLastLbid()
{
    if (!_isScan)
        return 0;

    return lastLbid[bpp->dbRoot - 1];

#if 0
    /* PL - each dbroot has a different HWM; need to look up the local HWM on start */
    BRM::DBRM dbrm;
    BRM::OID_t oid;
    uint32_t partNum;
    uint16_t segNum;
    uint32_t fbo;

    dbrm.lookupLocal(primMsg->LBID, bpp->versionNum, false, oid, dbRoot, partNum, segNum, fbo);
    gotDBRoot = true;
    cout << "I think I'm on dbroot " << dbRoot << " lbid=" << primMsg->LBID << " ver=" << bpp->versionNum << endl;
    dbRoot--;
    return lastLbid[dbRoot];
#endif
}

ColumnCommand* ColumnCommandFabric::createCommand(messageqcpp::ByteStream& bs)
{
    bs.advance(1); // The higher dispatcher Command::makeCommand calls BS::peek so this increments BS ptr
    execplan::ColumnCommandDataType colType;
    colType.unserialize(bs);
    switch (colType.colWidth)
    {
        case 1:
            return new ColumnCommandInt8(colType, bs);
            break;

        case 2:
           return new ColumnCommandInt16(colType, bs);
           break;

        case 4:
           return new ColumnCommandInt32(colType, bs);
           break;

        case 8:
           return new ColumnCommandInt64(colType, bs);
           break;

        case 16:
           return new ColumnCommandInt128(colType, bs);
           break;

        default:
           throw std::runtime_error("ColumnCommandFabric::createCommand: unsupported width " + colType.colWidth);
    }

    return nullptr;
}

ColumnCommand* ColumnCommandFabric::duplicate(ColumnCommandShPtr& rhs)
{
    return duplicate(rhs.get());
}

// reversed semantics comparing to the existing.
// duplicate the argument not this
ColumnCommand* ColumnCommandFabric::duplicate(ColumnCommand* rhs)
{
    if (LIKELY(typeid(*rhs) == typeid(ColumnCommandInt64)))
    {
        ColumnCommandInt64* ret = new ColumnCommandInt64();
        ColumnCommandInt64* src = dynamic_cast<ColumnCommandInt64*>(rhs);
        src->duplicate(ret);
        //*ret = *dynamic_cast<const ColumnCommandInt64*>(rhs);
        return ret;
    }
    else if (typeid(*rhs) == typeid(ColumnCommandInt128))
    {
        ColumnCommandInt128* ret = new ColumnCommandInt128();
        *ret = *dynamic_cast<ColumnCommandInt128*>(rhs);
        return ret;
    }
    else if (typeid(*rhs) == typeid(ColumnCommandInt8))
    {
        ColumnCommandInt8* ret = new ColumnCommandInt8();
        *ret = *dynamic_cast<ColumnCommandInt8*>(rhs);
        return ret;
    }
    else if (typeid(*rhs) == typeid(ColumnCommandInt16))
    {
        ColumnCommandInt16* ret = new ColumnCommandInt16();
        *ret = *dynamic_cast<ColumnCommandInt16*>(rhs);
        return ret;
    }
    else if (typeid(*rhs) == typeid(ColumnCommandInt32))
    {
        ColumnCommandInt32* ret = new ColumnCommandInt32();
        *ret = *dynamic_cast<ColumnCommandInt32*>(rhs);
        return ret;
    }
    else
        throw std::runtime_error("ColumnCommandFabric::duplicate: Can not detect ColumnCommand child class");

    return nullptr;
}

// Code duplication here for the future patch.
ColumnCommandInt8::ColumnCommandInt8(execplan::ColumnCommandDataType& aColType, messageqcpp::ByteStream& bs)
{
    ColumnCommand::createCommand(aColType, bs);
    parsedColumnFilter = primitives::parseColumnFilter(filterString.buf(), colType.colWidth,
                     colType.colDataType, filterCount, BOP);

    /* OR hack */
    emptyFilter = primitives::parseColumnFilter(filterString.buf(), colType.colWidth,
                      colType.colDataType, 0, BOP);
}

void ColumnCommandInt8::prep(int8_t outputType, bool absRids)
{
    shift = 16;
    mask = 0xFF;
    ColumnCommand::_prep(outputType, absRids);
}

ColumnCommandInt16::ColumnCommandInt16(execplan::ColumnCommandDataType& aColType, messageqcpp::ByteStream& bs)
{
    colType = aColType;
    ColumnCommand::createCommand(aColType, bs);
    parsedColumnFilter = primitives::parseColumnFilter(filterString.buf(),
                                                       colType.colWidth,
                                                       colType.colDataType,
                                                       filterCount, BOP);
    /* OR hack */
    emptyFilter = primitives::parseColumnFilter(filterString.buf(),
                                                colType.colWidth,
                                                colType.colDataType, 0, BOP);
}

void ColumnCommandInt16::prep(int8_t outputType, bool absRids)
{
    shift = 8;
    mask = 0xFF;
    ColumnCommand::_prep(outputType, absRids);
}

ColumnCommandInt32::ColumnCommandInt32(execplan::ColumnCommandDataType& aColType, messageqcpp::ByteStream& bs)
{
    ColumnCommand::createCommand(aColType, bs);
    parsedColumnFilter = primitives::parseColumnFilter(filterString.buf(), colType.colWidth,
                     colType.colDataType, filterCount, BOP);

    /* OR hack */
    emptyFilter = primitives::parseColumnFilter(filterString.buf(), colType.colWidth,
                      colType.colDataType, 0, BOP);
}

void ColumnCommandInt32::prep(int8_t outputType, bool absRids)
{
    shift = 4;
    mask = 0x0F;
    ColumnCommand::_prep(outputType, absRids);
}

ColumnCommandInt64::ColumnCommandInt64(execplan::ColumnCommandDataType& aColType, messageqcpp::ByteStream& bs)
{
    ColumnCommand::createCommand(aColType, bs);
    parsedColumnFilter = primitives::parseColumnFilter(filterString.buf(), colType.colWidth,
                     colType.colDataType, filterCount, BOP);

    /* OR hack */
    emptyFilter = primitives::parseColumnFilter(filterString.buf(), colType.colWidth,
                      colType.colDataType, 0, BOP);
}

void ColumnCommandInt64::prep(int8_t outputType, bool absRids)
{
    shift = 2;
    mask = 0x03;
    ColumnCommand::_prep(outputType, absRids);
}

ColumnCommandInt128::ColumnCommandInt128(execplan::ColumnCommandDataType& aColType, messageqcpp::ByteStream& bs)
{
    // WIP move this assignment into the class init list
    // There is a duplicate in createCommand
    colType = aColType;
    ColumnCommand::createCommand(aColType, bs);
    parsedColumnFilter = primitives::parseColumnFilter(filterString.buf(),
                                                       colType.colWidth,
                                                       colType.colDataType,
                                                       filterCount, BOP);
    /* OR hack */
    emptyFilter = primitives::parseColumnFilter(filterString.buf(),
                                                colType.colWidth,
                                                colType.colDataType, 0, BOP);
}

void ColumnCommandInt128::prep(int8_t outputType, bool absRids)
{
    shift = 1;
    mask = 0x01;
    ColumnCommand::_prep(outputType, absRids);
}

}
// vim:ts=4 sw=4:

