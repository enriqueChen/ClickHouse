#pragma once

#include <cstdlib>
#include <memory>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/ArenaAllocator.h>
#include "Columns/ColumnsNumber.h"
#include "DataTypes/DataTypesNumber.h"
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-exception-parameter"
#pragma GCC diagnostic ignored "-Wshadow"
#pragma GCC diagnostic ignored "-Wreserved-id-macro"
#pragma GCC diagnostic ignored "-Wextra-semi-stmt"
#pragma GCC diagnostic ignored "-Wold-style-cast"
#include <theta_sketch.hpp>
#include <theta_union.hpp>
#pragma GCC diagnostic pop

namespace DB {

namespace ErrorCodes {
    extern const int LOGICAL_ERROR;
};

using ThetaSketch = datasketches::theta_sketch; 
using CompactThetaSketch = datasketches::compact_theta_sketch; 
using UpdateThetaSketch = datasketches::update_theta_sketch;
using ThetaSketchPtr = std::shared_ptr<ThetaSketch>;
using CompactThetaSketchPtr = std::shared_ptr<CompactThetaSketch>;
using UpdateThetaSketchPtr = std::shared_ptr<UpdateThetaSketch>;

static  std::size_t ReadBufSize = 1024 * 1024; // 1M byte buffer
namespace detail {
struct AggregateFunctionThetaSketchTraits {

    // read bytes to compacted sketches 
    static ThetaSketchPtr read(ReadBuffer & buf){
        void *buffer = malloc(sizeof(char));
        std::size_t read = 0, copied = 0;
        do {
            buffer = realloc(buffer, ReadBufSize * sizeof(char));
            char* rp = reinterpret_cast<char*>(buffer);
            rp += copied;
            read = buf.read(rp, ReadBufSize);
            copied += read;
        } while( !buf.eof() );

        auto c = CompactThetaSketch::deserialize(buffer, copied);
        free(buffer);
        return std::make_shared<CompactThetaSketch>(c);
    }
    
    // write compacted sketches to bytes
    static void write(const ThetaSketchPtr s, WriteBuffer & buf) {
        if (s == nullptr) return;
        UpdateThetaSketchPtr usketch = std::dynamic_pointer_cast<UpdateThetaSketch>(s);
        CompactThetaSketchPtr csketch = nullptr; 
        if (usketch != nullptr) {
            csketch = std::make_shared<CompactThetaSketch>(usketch->compact());
        } else {
            csketch = std::dynamic_pointer_cast<CompactThetaSketch>(s);
        }
        if (csketch==nullptr) return;
        auto bytes = csketch->serialize(); 
        for (auto & b: bytes) {
            buf.write(b);
        }
    }

    // add key value to update-able sketches
    template <typename KeyType>
    static void add(ThetaSketchPtr sketch, const KeyType & value) {
        if (sketch == nullptr) {
            UpdateThetaSketch u = UpdateThetaSketch::builder().build();
            u.update(value);
            sketch = std::make_shared<UpdateThetaSketch>(u);
            return;
        }
        UpdateThetaSketchPtr u = std::dynamic_pointer_cast<UpdateThetaSketch>(sketch);
        if (u!=nullptr) {
            u->update(value);
            return;
        }

        // Could not update compact theta sketch
        throw Exception(
                "Aggregation function theta sketch could not support update for compacted sketches.", 
                ErrorCodes::LOGICAL_ERROR );
    }


    // merge two compacted sketches to one with tuple union
    static void merge(ThetaSketchPtr place, const ThetaSketchPtr rhs) {
        datasketches::theta_union u = datasketches::theta_union::builder().build();
        u.update(*place);
        u.update(*rhs);
        CompactThetaSketch ret = u.get_result();
        place = std::make_shared<CompactThetaSketch>(ret);
    }
    
    static double cardinality(const ThetaSketchPtr place) {
        return place->get_estimate();
    }
};

};

struct  AggregateFunctionThetaSketchData {
    ThetaSketchPtr sketch;
    AggregateFunctionThetaSketchData(): sketch(nullptr){}
};

template <typename KeyType>
class AggregateFunctionThetaSketch final
    : public IAggregateFunctionDataHelper<AggregateFunctionThetaSketchData, AggregateFunctionThetaSketch<KeyType> >{
public:
    AggregateFunctionThetaSketch(const DataTypes & argument_types_, const Array & params_)
        : IAggregateFunctionDataHelper<AggregateFunctionThetaSketchData, AggregateFunctionThetaSketch<KeyType>>(argument_types_, params_) {}

    String getName() const override { 
        return "thetaSketch";
    }

    DataTypePtr getReturnType() const  override {
        return std::make_shared<DataTypeUInt64>();
    } 
    
    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override {
        if constexpr (!std::is_same_v<KeyType, String>) {
            const auto & value = assert_cast<const ColumnVector<KeyType> &>(*columns[0]).getElement(row_num);
            detail::AggregateFunctionThetaSketchTraits::add<KeyType>(this->data(place).sketch, value);       
        } else {
            StringRef value = columns[0]->getDataAt(row_num);
            detail::AggregateFunctionThetaSketchTraits::add<String>(this->data(place).sketch, value.data);
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override {
        detail::AggregateFunctionThetaSketchTraits::merge(this->data(place).sketch, this->data(rhs).sketch);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override {
        detail::AggregateFunctionThetaSketchTraits::write(this->data(place).sketch, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override {
        this->data(place).sketch = detail::AggregateFunctionThetaSketchTraits::read(buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override {
        assert_cast<ColumnUInt64 &>(to).getData().push_back( std::round( detail::AggregateFunctionThetaSketchTraits::cardinality( this->data(place).sketch ) ) );
    }
};

};
