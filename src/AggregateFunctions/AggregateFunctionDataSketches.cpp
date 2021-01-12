#include <AggregateFunctions/AggregateFunctionDataSketches.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include "common/types.h"

#include <functional>
#include "AggregateFunctions/FactoryHelpers.h"
#include "registerAggregateFunctions.h"

namespace DB{

namespace ErrorCodes {
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
};

namespace {

    AggregateFunctionPtr createAggregateFunctionThetaSketch(const std::string & name, const DataTypes &arguement_types, const Array & params){
        assertUnary(name, arguement_types);
        
        const IDataType & arguement_type  = * arguement_types[0];
        
        WhichDataType which(arguement_type);
        if (which.idx == TypeIndex::Int8) return std::make_shared<DB::template AggregateFunctionThetaSketch<Int8> >(arguement_types, params);
        if (which.idx == TypeIndex::UInt8) return std::make_shared<DB::template AggregateFunctionThetaSketch<UInt8> >(arguement_types, params);
        if (which.idx == TypeIndex::Int16) return std::make_shared<DB::template AggregateFunctionThetaSketch<Int16> >(arguement_types, params);
        if (which.idx == TypeIndex::UInt16) return std::make_shared<DB::template AggregateFunctionThetaSketch<UInt16> >(arguement_types, params);
        if (which.idx == TypeIndex::Int32) return std::make_shared<DB::template AggregateFunctionThetaSketch<Int32> >(arguement_types, params);
        if (which.idx == TypeIndex::UInt32) return std::make_shared<DB::template AggregateFunctionThetaSketch<UInt32> >(arguement_types, params);
        if (which.idx == TypeIndex::Int64) return std::make_shared<DB::template AggregateFunctionThetaSketch<Int64> >(arguement_types, params);
        if (which.idx == TypeIndex::UInt64) return std::make_shared<DB::template AggregateFunctionThetaSketch<UInt64> >(arguement_types, params);
        if (which.idx == TypeIndex::Float32) return std::make_shared<DB::template AggregateFunctionThetaSketch<Float32> >(arguement_types, params);
        if (which.idx == TypeIndex::Float64) return std::make_shared<DB::template AggregateFunctionThetaSketch<Float64> >(arguement_types, params);
        if (which.idx == TypeIndex::String) return std::make_shared<DB::template AggregateFunctionThetaSketch<String> >(arguement_types, params);
        throw Exception("Aggregate function" + name + "must have basic numeric types or string", 
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        
    }
};

void registerAggregateFunctionDataSketches(AggregateFunctionFactory & factory) {
    using namespace std::placeholders;
    factory.registerFunction("thetaSketch", std::bind(createAggregateFunctionThetaSketch, _1, _2, _3));
}

};
