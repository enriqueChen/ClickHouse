#include "config_core.h"
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionJIT.h>
#include <Interpreters/Join.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <set>
#include <optional>


namespace ProfileEvents
{
    extern const Event FunctionExecute;
    extern const Event CompiledFunctionExecute;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_COLUMN;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_ACTION;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int TOO_MANY_TEMPORARY_COLUMNS;
    extern const int TOO_MANY_TEMPORARY_NON_CONST_COLUMNS;
    extern const int TYPE_MISMATCH;
}


Names ExpressionAction::getNeededColumns() const
{
    Names res;

    for (const auto & name_with_position : argument_names)
        res.emplace_back(name_with_position);

    for (const auto & name_with_position : array_joined_columns)
        res.emplace_back(name_with_position.first);

    res.insert(res.end(), join_key_names_left.begin(), join_key_names_left.end());

    for (const auto & name_with_position : projection_names)
        res.emplace_back(name_with_position);

    if (!source_name.name.empty())
        res.push_back(source_name);

    return res;
}


void ExpressionAction::enumerateColumns(EnumeratedColumns & enumerated_columns)
{
    auto add = [&](const std::string & name, size_t & position)
    {
        if (name.empty())
            return;

        size_t number = enumerated_columns.size();
        auto it = enumerated_columns.emplace(name, number);
        position = it.first->second;
    };

    for (auto & name_with_position : argument_names)
        add(name_with_position.name, name_with_position.position);

    for (auto & name_with_position : array_joined_columns)
        add(name_with_position.first, name_with_position.second);

    for (auto & name_with_position : projection_names)
        add(name_with_position.name, name_with_position.position);

    for (auto & name_with_position : projection_aliases)
        add(name_with_position.name, name_with_position.position);

    add(source_name.name, source_name.position);
    add(result_name.name, result_name.position);
}


ExpressionAction ExpressionAction::applyFunction(
    const FunctionBuilderPtr & function_,
    const std::vector<std::string> & argument_names_,
    std::string result_name_)
{
    if (result_name_.empty())
    {
        result_name_ = function_->getName() + "(";
        for (size_t i = 0 ; i < argument_names_.size(); ++i)
        {
            if (i)
                result_name_ += ", ";
            result_name_ += argument_names_[i];
        }
        result_name_ += ")";
    }

    ExpressionAction a;
    a.type = APPLY_FUNCTION;
    a.result_name = result_name_;
    a.function_builder = function_;

    a.argument_names.reserve(argument_names_.size());
    for (const auto & name : argument_names_)
        a.argument_names.emplace_back(name);

    return a;
}

ExpressionAction ExpressionAction::addColumn(
    const ColumnWithTypeAndName & added_column_)
{
    ExpressionAction a;
    a.type = ADD_COLUMN;
    a.result_name = added_column_.name;
    a.result_type = added_column_.type;
    a.added_column = added_column_.column;
    return a;
}

ExpressionAction ExpressionAction::removeColumn(const std::string & removed_name)
{
    ExpressionAction a;
    a.type = REMOVE_COLUMN;
    a.source_name = removed_name;
    return a;
}

ExpressionAction ExpressionAction::copyColumn(const std::string & from_name, const std::string & to_name, bool can_replace)
{
    ExpressionAction a;
    a.type = COPY_COLUMN;
    a.source_name = from_name;
    a.result_name = to_name;
    a.can_replace = can_replace;
    return a;
}

ExpressionAction ExpressionAction::project(const NamesWithAliases & projected_columns_)
{
    ExpressionAction a;
    a.type = PROJECT;
    a.projection_names.resize(projected_columns_.size());
    a.projection_aliases.resize(projected_columns_.size());
    for (size_t i = 0; i < projected_columns_.size(); ++i)
    {
        a.projection_names[i] = projected_columns_[i].first;
        a.projection_aliases[i] = projected_columns_[i].second;
    }
    return a;
}

ExpressionAction ExpressionAction::project(const Names & projected_columns_)
{
    ExpressionAction a;
    a.type = PROJECT;
    a.projection_names.resize(projected_columns_.size());
    a.projection_aliases.resize(projected_columns_.size());
    for (size_t i = 0; i < projected_columns_.size(); ++i)
        a.projection_names[i] = projected_columns_[i];
    return a;
}

ExpressionAction ExpressionAction::addAliases(const NamesWithAliases & aliased_columns_)
{
    ExpressionAction a = project(aliased_columns_);
    a.type = ADD_ALIASES;
    return a;
}

ExpressionAction ExpressionAction::arrayJoin(const NameSet & array_joined_columns, bool array_join_is_left, const Context & context)
{
    if (array_joined_columns.empty())
        throw Exception("No arrays to join", ErrorCodes::LOGICAL_ERROR);
    ExpressionAction a;
    a.type = ARRAY_JOIN;

    for (auto & column : array_joined_columns)
        a.array_joined_columns.emplace(column);

    a.array_join_is_left = array_join_is_left;
    a.unaligned_array_join = context.getSettingsRef().enable_unaligned_array_join;

    if (a.unaligned_array_join)
    {
        a.function_length = FunctionFactory::instance().get("length", context);
        a.function_greatest = FunctionFactory::instance().get("greatest", context);
        a.function_arrayResize = FunctionFactory::instance().get("arrayResize", context);
    }
    else if (array_join_is_left)
        a.function_builder = FunctionFactory::instance().get("emptyArrayToSingle", context);

    return a;
}

ExpressionAction ExpressionAction::ordinaryJoin(
    std::shared_ptr<const Join> join_,
    const Names & join_key_names_left,
    const NamesAndTypesList & columns_added_by_join_)
{
    ExpressionAction a;
    a.type = JOIN;
    a.join = std::move(join_);
    a.join_key_names_left = join_key_names_left;
    a.columns_added_by_join = columns_added_by_join_;
    return a;
}


void ExpressionAction::prepare(Block & sample_block, const Settings & settings)
{
    // std::cerr << "preparing: " << toString() << std::endl;

    /** Constant expressions should be evaluated, and put the result in sample_block.
      */

    switch (type)
    {
        case APPLY_FUNCTION:
        {
            if (sample_block.has(result_name))
                throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

            bool all_const = true;

            ColumnNumbers arguments(argument_names.size());
            for (size_t i = 0; i < argument_names.size(); ++i)
            {
                arguments[i] = sample_block.getPositionByName(argument_names[i]);
                ColumnPtr col = sample_block.safeGetByPosition(arguments[i]).column;
                if (!col || !isColumnConst(*col))
                    all_const = false;
            }

            size_t result_position = sample_block.columns();
            sample_block.insert({nullptr, result_type, result_name});
            function = function_base->prepare(sample_block, arguments, result_position);

            if (auto * prepared_function = dynamic_cast<PreparedFunctionImpl *>(function.get()))
                prepared_function->createLowCardinalityResultCache(settings.max_threads);

            bool compile_expressions = false;
#if USE_EMBEDDED_COMPILER
            compile_expressions = settings.compile_expressions;
#endif
            /// If all arguments are constants, and function is suitable to be executed in 'prepare' stage - execute function.
            /// But if we compile expressions compiled version of this function maybe placed in cache,
            /// so we don't want to unfold non deterministic functions
            if (all_const && function_base->isSuitableForConstantFolding() && (!compile_expressions || function_base->isDeterministic()))
            {
                function->execute(sample_block, arguments, result_position, sample_block.rows(), true);

                /// If the result is not a constant, just in case, we will consider the result as unknown.
                ColumnWithTypeAndName & col = sample_block.safeGetByPosition(result_position);
                if (!isColumnConst(*col.column))
                {
                    col.column = nullptr;
                }
                else
                {
                    /// All constant (literal) columns in block are added with size 1.
                    /// But if there was no columns in block before executing a function, the result has size 0.
                    /// Change the size to 1.

                    if (col.column->empty())
                        col.column = col.column->cloneResized(1);
                }
            }

            break;
        }

        case ARRAY_JOIN:
        {
            for (const auto & name : array_joined_columns)
            {
                ColumnWithTypeAndName & current = sample_block.getByName(name.first);
                const auto * array_type = typeid_cast<const DataTypeArray *>(&*current.type);
                if (!array_type)
                    throw Exception("ARRAY JOIN requires array argument", ErrorCodes::TYPE_MISMATCH);
                current.type = array_type->getNestedType();
                current.column = nullptr;
            }

            break;
        }

        case JOIN:
        {
            /// TODO join_use_nulls setting

            for (const auto & col : columns_added_by_join)
                sample_block.insert(ColumnWithTypeAndName(nullptr, col.type, col.name));

            break;
        }

        case PROJECT:
        {
            Block new_block;

            for (size_t i = 0; i < projection_names.size(); ++i)
            {
                const std::string & name = projection_names[i];
                const std::string & alias = projection_aliases[i];
                ColumnWithTypeAndName column = sample_block.getByName(name);
                if (!alias.empty())
                    column.name = alias;
                new_block.insert(std::move(column));
            }

            sample_block.swap(new_block);
            break;
        }

        case ADD_ALIASES:
        {
            for (size_t i = 0; i < projection_names.size(); ++i)
            {
                const std::string & name = projection_names[i];
                const std::string & alias = projection_aliases[i];
                const ColumnWithTypeAndName & column = sample_block.getByName(name);
                if (!alias.empty() && !sample_block.has(alias))
                    sample_block.insert({column.column, column.type, alias});
            }
            break;
        }

        case REMOVE_COLUMN:
        {
            sample_block.erase(source_name);
            break;
        }

        case ADD_COLUMN:
        {
            if (sample_block.has(result_name))
                throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

            sample_block.insert(ColumnWithTypeAndName(added_column, result_type, result_name));
            break;
        }

        case COPY_COLUMN:
        {
            const auto & source = sample_block.getByName(source_name);
            result_type = source.type;

            if (sample_block.has(result_name))
            {
                if (can_replace)
                {
                    auto & result = sample_block.getByName(result_name);
                    result.type = result_type;
                    result.column = source.column;
                }
                else
                    throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);
            }
            else
                sample_block.insert(ColumnWithTypeAndName(source.column, result_type, result_name));

            break;
        }
    }
}


template <bool execute_on_block>
void ExpressionAction::execute(
    Block & block,
    Columns & columns,
    size_t & num_rows,
    ColumnNumbers & index,
    const EnumeratedColumns & enumerated_columns,
    bool dry_run) const
{
    /*
     * Double-indexation schema is used.
     * Map `enumerated_columns` contains all columns which could be used in actions.
     * Each of this column has unique number 0 to enumerated_columns.size() - 1.
     *
     * Index array contains positions in block for each enumerated column (or INDEX_NOT_FOUND).
     * Each action also affects index array.
     *
     * Action can be executed on columns or on block itself (in case if execute_on_block is true).
     * When action is executed on columns, block structure remains unchanged.
     * Otherwise, columns argument is ignored and block is changed (which also allows to get header for the next step).
     */

    size_t input_rows_count = num_rows;

    auto getIndex = [&index](const NameWithPosition & name) { return index[name.position]; };
    auto getIndexAt = [&index](const NamesWithPosition & names, size_t i) { return index[names[i].position]; };

    auto setIndex = [&index](const NameWithPosition & name, size_t value) { index[name.position] = value; };

    auto getColumn = [&](size_t pos) -> ColumnPtr &
    {
        if constexpr (execute_on_block)
            return block.getByPosition(pos).column;
        else
            return columns[pos];
    };


    if (type == REMOVE_COLUMN || type == COPY_COLUMN)
        if (getIndex(source_name) == INDEX_NOT_FOUND)
            throw Exception("Not found column '" + source_name + "'. There are columns: " + block.dumpNames(),
                            ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

    if (type == ADD_COLUMN || (type == COPY_COLUMN && !can_replace) || type == APPLY_FUNCTION)
        if (getIndex(result_name) != INDEX_NOT_FOUND)
            throw Exception("Column '" + result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

    switch (type)
    {
        case APPLY_FUNCTION:
        {
            ColumnNumbers arguments(argument_names.size());
            for (size_t i = 0; i < argument_names.size(); ++i)
            {
                auto pos = getIndexAt(argument_names, i);
                arguments[i] = pos;

                if (pos == INDEX_NOT_FOUND)
                    throw Exception("Not found column: '" + argument_names[i] + "'",
                                    ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

                if constexpr (!execute_on_block)
                    block.getByPosition(pos).column.swap(columns[pos]);
            }

            size_t num_columns_without_result = block.columns();
            block.insert({ nullptr, result_type, result_name});

            ProfileEvents::increment(ProfileEvents::FunctionExecute);
            if (is_function_compiled)
                ProfileEvents::increment(ProfileEvents::CompiledFunctionExecute);
            function->execute(block, arguments, num_columns_without_result, input_rows_count, dry_run);

            setIndex(result_name, num_columns_without_result);

            if constexpr (!execute_on_block)
            {
                columns.emplace_back(std::move(block.getByPosition(num_columns_without_result).column));
                block.erase(num_columns_without_result);

                for (auto & arg : arguments)
                    block.getByPosition(arg).column.swap(columns[arg]);
            }

            break;
        }

        case ARRAY_JOIN:
        {
            if (array_joined_columns.empty())
                throw Exception("No arrays to join", ErrorCodes::LOGICAL_ERROR);

            if (unaligned_array_join)
            {
                /// Resize all array joined columns to the longest one, (at least 1 if LEFT ARRAY JOIN), padded with default values.
                auto rows = block.rows();
                auto uint64 = std::make_shared<DataTypeUInt64>();
                ColumnWithTypeAndName column_of_max_length;
                if (array_join_is_left)
                    column_of_max_length = ColumnWithTypeAndName(uint64->createColumnConst(rows, 1u), uint64, {});
                else
                    column_of_max_length = ColumnWithTypeAndName(uint64->createColumnConst(rows, 0u), uint64, {});

                for (const auto & name : array_joined_columns)
                {
                    auto src_index = index[name.second];
                    auto & src_col = block.getByPosition(src_index);

                    if constexpr (!execute_on_block)
                        src_col.column.swap(columns[src_index]);

                    /// Calculate length(array).
                    Block tmp_block{src_col, {{}, uint64, {}}};
                    function_length->build({src_col})->execute(tmp_block, {0}, 1, rows);

                    /// column_of_max_length = max(length(array), column_of_max_length)
                    Block tmp_block2{
                        column_of_max_length, tmp_block.safeGetByPosition(1), {{}, uint64, {}}};
                    function_greatest->build({column_of_max_length, tmp_block.safeGetByPosition(1)})->execute(tmp_block2, {0, 1}, 2, rows);
                    column_of_max_length = tmp_block2.safeGetByPosition(2);

                    if constexpr (!execute_on_block)
                        src_col.column.swap(columns[src_index]);
                }

                for (const auto & name : array_joined_columns)
                {
                    auto src_index = index[name.second];
                    auto & src_col = block.getByPosition(src_index);

                    if constexpr (!execute_on_block)
                        src_col.column.swap(columns[src_index]);

                    /// arrayResize(src_col, column_of_max_length)
                    Block tmp_block{src_col, column_of_max_length, {{}, src_col.type, {}}};
                    function_arrayResize->build({src_col, column_of_max_length})->execute(tmp_block, {0, 1}, 2, rows);
                    src_col.column = tmp_block.safeGetByPosition(2).column;

                    if constexpr (!execute_on_block)
                        src_col.column.swap(columns[src_index]);
                }
            }
            else if (array_join_is_left)
            {
                for (const auto & name : array_joined_columns)
                {
                    auto src_index = index[name.second];
                    auto & src_col = block.getByPosition(src_index);

                    if constexpr (!execute_on_block)
                        src_col.column.swap(columns[src_index]);

                    Block tmp_block{src_col, {{}, src_col.type, {}}};
                    function_builder->build({src_col})->execute(tmp_block, {0}, 1, src_col.column->size(), dry_run);
                    src_col.column = tmp_block.safeGetByPosition(1).column;

                    if constexpr (!execute_on_block)
                        src_col.column.swap(columns[src_index]);
                }
            }

            ColumnPtr any_array_ptr = getColumn(index[array_joined_columns.begin()->second])->convertToFullColumnIfConst();
            const auto * any_array = typeid_cast<const ColumnArray *>(&*any_array_ptr);
            if (!any_array)
                throw Exception("ARRAY JOIN of not array: " + array_joined_columns.begin()->first, ErrorCodes::TYPE_MISMATCH);

            size_t num_columns = block.columns();
            for (size_t i = 0; i < num_columns; ++i)
            {
                ColumnWithTypeAndName & current = block.getByPosition(i);
                auto & column = getColumn(i);

                if (array_joined_columns.count(current.name))
                {
                    if (!typeid_cast<const DataTypeArray *>(&*current.type))
                        throw Exception("ARRAY JOIN of not array: " + current.name, ErrorCodes::TYPE_MISMATCH);

                    ColumnPtr array_ptr = column->convertToFullColumnIfConst();

                    const ColumnArray & array = typeid_cast<const ColumnArray &>(*array_ptr);
                    if (!unaligned_array_join && !array.hasEqualOffsets(typeid_cast<const ColumnArray &>(*any_array_ptr)))
                        throw Exception("Sizes of ARRAY-JOIN-ed arrays do not match", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

                    column = typeid_cast<const ColumnArray &>(*array_ptr).getDataPtr();

                    if constexpr (execute_on_block)
                        current.type = typeid_cast<const DataTypeArray &>(*current.type).getNestedType();
                }
                else
                {
                    column = column->replicate(any_array->getOffsets());
                }

                if (i == 0)
                    num_rows = column->size();
            }

            break;
        }

        case JOIN:
        {
            if constexpr (!execute_on_block)
            {
                for (size_t i = 0; i < columns.size(); ++i)
                    block.getByPosition(i).column.swap(columns[i]);
            }

            join->joinBlock(block, join_key_names_left, columns_added_by_join);

            if constexpr (!execute_on_block)
            {
                columns.resize(block.columns());

                for (size_t i = 0; i < columns.size(); ++i)
                    block.getByPosition(i).column.swap(columns[i]);
            }

            num_rows = block.rows();
            index = makeIndex(block, enumerated_columns);

            break;
        }

        case PROJECT:
        {
            Block new_block;
            Columns new_columns;
            ColumnNumbers new_index(index.size(), INDEX_NOT_FOUND);

            for (size_t i = 0; i < projection_names.size(); ++i)
            {
                const std::string & alias = projection_aliases[i];

                auto pos = getIndex(projection_names[i]);

                if constexpr (execute_on_block)
                {
                    ColumnWithTypeAndName column = block.getByPosition(pos);
                    if (!alias.empty())
                        column.name = alias;

                    new_block.insert(std::move(column));
                }
                else
                    new_columns.emplace_back(columns[pos]);

                if (alias.empty())
                    new_index[projection_names[i].position] = i;
                else
                    new_index[projection_aliases[i].position] = i;
            }

            index.swap(new_index);

            if constexpr (execute_on_block)
                block.swap(new_block);
            else
                columns.swap(new_columns);

            break;
        }

        case ADD_ALIASES:
        {
            size_t num_columns = block.columns();

            for (size_t i = 0; i < projection_names.size(); ++i)
            {
                const std::string & alias = projection_aliases[i];

                if (alias.empty())
                    continue;

                auto pos = getIndex(projection_names[i]);
                index[projection_aliases[i].position] = num_columns;
                ++num_columns;

                if constexpr (execute_on_block)
                    block.insert(block.getByPosition(pos));
                else
                    columns.emplace_back(columns[pos]);
            }
            break;
        }

        case REMOVE_COLUMN:
        {
            auto pos = getIndex(source_name);

            if constexpr (execute_on_block)
                block.erase(pos);
            else
                columns.erase(columns.begin() + pos);

            index[source_name.position] = INDEX_NOT_FOUND;

            for (auto & val : index)
                if (val > pos && val != INDEX_NOT_FOUND)
                    --val;

            break;
        }

        case ADD_COLUMN:
        {
            auto pos = result_name.position;
            index[pos] = block.columns();

            if constexpr (execute_on_block)
                block.insert({ added_column->cloneResized(input_rows_count), result_type, result_name });
            else
                columns.emplace_back(added_column->cloneResized(input_rows_count));

            break;
        }

        case COPY_COLUMN:
        {
            auto dst_pos = getIndex(result_name);
            auto src_pos = getIndex(source_name);

            if (can_replace && dst_pos != INDEX_NOT_FOUND)
            {
                if constexpr (execute_on_block)
                {
                    auto & dst_col = block.getByPosition(dst_pos);
                    auto & src_col = block.getByPosition(src_pos);
                    dst_col.column = src_col.column;
                    dst_col.type = src_col.type;
                }
                else
                    columns[dst_pos] = columns[src_pos];
            }
            else
            {
                index[result_name.position] = block.columns();

                if constexpr (execute_on_block)
                    block.insert({ block.getByPosition(src_pos).column, result_type, result_name });
                else
                    columns.emplace_back(columns[src_pos]);
            }

            break;
        }
    }
}


ColumnNumbers ExpressionAction::makeIndex(const Block & block, const EnumeratedColumns & enumerated_columns)
{
    ColumnNumbers index(enumerated_columns.size(), INDEX_NOT_FOUND);

    for (size_t i = 0, size = block.columns(); i < size; ++i)
    {
        const auto & column = block.getByPosition(i);
        auto it = enumerated_columns.find(column.name);
        if (it != enumerated_columns.end())
            index[it->second] = i;
    }

    return index;
}


template <bool execute_on_block>
void ExpressionAction::executeOnTotals(
    Block & block,
    Columns & columns,
    ColumnNumbers & index,
    const EnumeratedColumns & enumerated_columns) const
{
    if (type != JOIN)
    {
        size_t num_rows = 1;
        execute<execute_on_block>(block, columns, num_rows, index, enumerated_columns, false);
    }
    else
    {
        if constexpr (!execute_on_block)
        {
            for (size_t i = 0; i < columns.size(); ++i)
                block.getByPosition(i).column.swap(columns[i]);
        }

        join->joinTotals(block);

        if constexpr (!execute_on_block)
        {
            for (size_t i = 0; i < columns.size(); ++i)
                block.getByPosition(i).column.swap(columns[i]);
        }

        index.assign(index.size(), INDEX_NOT_FOUND);
        for (size_t i = 0, size = block.columns(); i < size; ++i)
        {
            const auto & column = block.getByPosition(i);
            auto it = enumerated_columns.find(column.name);
            if (it != enumerated_columns.end())
                index[it->second] = i;
        }
    }
}


std::string ExpressionAction::toString() const
{
    std::stringstream ss;
    switch (type)
    {
        case ADD_COLUMN:
            ss << "ADD " << result_name << " "
                << (result_type ? result_type->getName() : "(no type)") << " "
                << (added_column ? added_column->getName() : "(no column)");
            break;

        case REMOVE_COLUMN:
            ss << "REMOVE " << source_name;
            break;

        case COPY_COLUMN:
            ss << "COPY " << result_name << " = " << source_name;
            if (can_replace)
                ss << " (can replace)";
            break;

        case APPLY_FUNCTION:
            ss << "FUNCTION " << result_name << " " << (is_function_compiled ? "[compiled] " : "")
                << (result_type ? result_type->getName() : "(no type)") << " = "
                << (function_base ? function_base->getName() : "(no function)") << "(";
            for (size_t i = 0; i < argument_names.size(); ++i)
            {
                if (i)
                    ss << ", ";
                ss << argument_names[i];
            }
            ss << ")";
            break;

        case ARRAY_JOIN:
            ss << (array_join_is_left ? "LEFT " : "") << "ARRAY JOIN ";
            for (auto it = array_joined_columns.begin(); it != array_joined_columns.end(); ++it)
            {
                if (it != array_joined_columns.begin())
                    ss << ", ";
                ss << it->first;
            }
            break;

        case JOIN:
            ss << "JOIN ";
            for (auto it = columns_added_by_join.begin(); it != columns_added_by_join.end(); ++it)
            {
                if (it != columns_added_by_join.begin())
                    ss << ", ";
                ss << it->name;
            }
            break;

        case PROJECT: [[fallthrough]];
        case ADD_ALIASES:
            ss << (type == PROJECT ? "PROJECT " : "ADD_ALIASES ");
            for (size_t i = 0; i < projection_names.size(); ++i)
            {
                if (i)
                    ss << ", ";
                ss << projection_names[i];
                if (!projection_aliases[i].name.empty() && projection_aliases[i].name != projection_names[i].name)
                    ss << " AS " << projection_aliases[i];
            }
            break;
    }

    return ss.str();
}

template <bool execute_on_block>
void ExpressionActions::checkLimits(const Block & header, const Columns & columns) const
{
    auto getColumn = [&](size_t i) -> const ColumnPtr &
    {
        if constexpr (execute_on_block)
            return header.getByPosition(i).column;
        else
            return columns[i];
    };

    if (settings.max_temporary_columns && header.columns() > settings.max_temporary_columns)
        throw Exception("Too many temporary columns: " + header.dumpNames()
            + ". Maximum: " + settings.max_temporary_columns.toString(),
            ErrorCodes::TOO_MANY_TEMPORARY_COLUMNS);

    if (settings.max_temporary_non_const_columns)
    {
        size_t non_const_columns = 0;
        for (size_t i = 0, size = header.columns(); i < size; ++i)
            if (getColumn(i) && !isColumnConst(*getColumn(i)))
                ++non_const_columns;

        if (non_const_columns > settings.max_temporary_non_const_columns)
        {
            std::stringstream list_of_non_const_columns;
            for (size_t i = 0, size = header.columns(); i < size; ++i)
                if (getColumn(i) && !isColumnConst(*getColumn(i)))
                    list_of_non_const_columns << "\n" << header.safeGetByPosition(i).name;

            throw Exception("Too many temporary non-const columns:" + list_of_non_const_columns.str()
                + ". Maximum: " + settings.max_temporary_non_const_columns.toString(),
                ErrorCodes::TOO_MANY_TEMPORARY_NON_CONST_COLUMNS);
        }
    }
}

void ExpressionActions::addInput(const ColumnWithTypeAndName & column)
{
    input_columns.emplace_back(column.name, column.type);
    sample_block.insert(column);
}

void ExpressionActions::addInput(const NameAndTypePair & column)
{
    addInput(ColumnWithTypeAndName(nullptr, column.type, column.name));
}

void ExpressionActions::add(const ExpressionAction & action, Names & out_new_columns)
{
    addImpl(action, out_new_columns);
}

void ExpressionActions::add(const ExpressionAction & action)
{
    Names new_names;
    addImpl(action, new_names);
}

void ExpressionActions::addImpl(ExpressionAction action, Names & new_names)
{
    if (!action.result_name.name.empty())
        new_names.push_back(action.result_name);
    new_names.insert(new_names.end(), action.array_joined_columns.begin(), action.array_joined_columns.end());

    /// Compiled functions are custom functions and them don't need building
    if (action.type == ExpressionAction::APPLY_FUNCTION && !action.is_function_compiled)
    {
        if (sample_block.has(action.result_name))
            throw Exception("Column '" + action.result_name + "' already exists", ErrorCodes::DUPLICATE_COLUMN);

        ColumnsWithTypeAndName arguments(action.argument_names.size());
        for (size_t i = 0; i < action.argument_names.size(); ++i)
        {
            if (!sample_block.has(action.argument_names[i]))
                throw Exception("Unknown identifier: '" + action.argument_names[i] + "'", ErrorCodes::UNKNOWN_IDENTIFIER);
            arguments[i] = sample_block.getByName(action.argument_names[i]);
        }

        action.function_base = action.function_builder->build(arguments);
        action.result_type = action.function_base->getReturnType();
    }

    if (action.type == ExpressionAction::ADD_ALIASES)
        for (const auto & name : action.projection_aliases)
            new_names.emplace_back(name);

    action.prepare(sample_block, settings);
    actions.push_back(action);
    actions.back().enumerateColumns(enumerated_columns);
}

void ExpressionActions::prependProjectInput()
{
    actions.insert(actions.begin(), ExpressionAction::project(getRequiredColumns()));
    actions[0].enumerateColumns(enumerated_columns);
}

void ExpressionActions::prependArrayJoin(const ExpressionAction & action, const Block & sample_block_before)
{
    if (action.type != ExpressionAction::ARRAY_JOIN)
        throw Exception("ARRAY_JOIN action expected", ErrorCodes::LOGICAL_ERROR);

    NameSet array_join_set;
    for (auto & name : action.array_joined_columns)
        array_join_set.insert(name.first);

    for (auto & it : input_columns)
    {
        if (array_join_set.count(it.name))
        {
            array_join_set.erase(it.name);
            it.type = std::make_shared<DataTypeArray>(it.type);
        }
    }
    for (const std::string & name : array_join_set)
    {
        input_columns.emplace_back(name, sample_block_before.getByName(name).type);
        actions.insert(actions.begin(), ExpressionAction::removeColumn(name));
        actions[0].enumerateColumns(enumerated_columns);
    }

    actions.insert(actions.begin(), action);
    actions[0].enumerateColumns(enumerated_columns);
    optimizeArrayJoin();
}


bool ExpressionActions::popUnusedArrayJoin(const Names & required_columns, ExpressionAction & out_action)
{
    if (actions.empty() || actions.back().type != ExpressionAction::ARRAY_JOIN)
        return false;
    NameSet required_set(required_columns.begin(), required_columns.end());
    for (const auto & name : actions.back().array_joined_columns)
    {
        if (required_set.count(name.first))
            return false;
    }
    for (const auto & name : actions.back().array_joined_columns)
    {
        DataTypePtr & type = sample_block.getByName(name.first).type;
        type = std::make_shared<DataTypeArray>(type);
    }
    out_action = actions.back();
    actions.pop_back();
    return true;
}

void ExpressionActions::execute(Block & block, bool dry_run) const
{
    Columns columns;
    size_t num_rows = block.rows();
    auto index = ExpressionAction::makeIndex(block, enumerated_columns);

    for (auto & action : actions)
    {
        action.execute<true>(block, columns, num_rows, index, enumerated_columns, dry_run);
        checkLimits<true>(block, columns);
    }
}

void ExpressionActions::execute(const Block & header, Columns & columns, size_t & num_rows, Cache & cache, bool dry_run) const
{
    if (!cache.is_initialized)
    {
        cache.is_initialized = true;
        cache.headers.clear();
        cache.headers.reserve(actions.size());

        auto index = ExpressionAction::makeIndex(header, enumerated_columns);
        auto block = header.cloneWithColumns(columns);

        /// Must copy because local index will be changed.
        cache.index = index;

        for (auto & action : actions)
        {
            cache.headers.emplace_back(block.cloneEmpty());
            action.execute<true>(block, columns, num_rows, index, enumerated_columns, dry_run);
            checkLimits<true>(block, columns);
        }

        columns = block.getColumns();
    }
    else
    {
        ColumnNumbers index = cache.index;

        for (size_t i = 0, size = actions.size(); i < size; ++i)
        {
            actions[i].execute<false>(cache.headers[i], columns, num_rows, index, enumerated_columns, dry_run);
            checkLimits<false>(cache.headers[i], columns);
        }
    }
}

bool ExpressionActions::hasTotalsInJoin() const
{
    bool has_totals_in_join = false;
    for (const auto & action : actions)
    {
        if (action.join && action.join->hasTotals())
        {
            has_totals_in_join = true;
            break;
        }
    }

    return has_totals_in_join;
}

void ExpressionActions::executeOnTotals(Block & block) const
{
    /// If there is `totals` in the subquery for JOIN, but we do not have totals, then take the block with the default values instead of `totals`.
    if (!block)
    {
        if (hasTotalsInJoin())
        {
            for (const auto & name_and_type : input_columns)
            {
                auto column = name_and_type.type->createColumn();
                column->insertDefault();
                block.insert(ColumnWithTypeAndName(std::move(column), name_and_type.type, name_and_type.name));
            }
        }
        else
            return; /// There's nothing to JOIN.
    }

    Columns columns;
    auto index = ExpressionAction::makeIndex(block, enumerated_columns);

    for (auto & action : actions)
    {
        action.executeOnTotals<true>(block, columns, index, enumerated_columns);
        checkLimits<true>(block, columns);
    }
}

std::string ExpressionActions::getSmallestColumn(const NamesAndTypesList & columns)
{
    std::optional<size_t> min_size;
    String res;

    for (const auto & column : columns)
    {
        /// @todo resolve evil constant
        size_t size = column.type->haveMaximumSizeOfValue() ? column.type->getMaximumSizeOfValueInMemory() : 100;

        if (!min_size || size < *min_size)
        {
            min_size = size;
            res = column.name;
        }
    }

    if (!min_size)
        throw Exception("No available columns", ErrorCodes::LOGICAL_ERROR);

    return res;
}

void ExpressionActions::finalize(const Names & output_columns)
{
    NameSet final_columns;
    for (const auto & name : output_columns)
    {
        if (!sample_block.has(name))
            throw Exception("Unknown column: " + name + ", there are only columns "
                            + sample_block.dumpNames(), ErrorCodes::UNKNOWN_IDENTIFIER);
        final_columns.insert(name);
    }

#if USE_EMBEDDED_COMPILER
    /// This has to be done before removing redundant actions and inserting REMOVE_COLUMNs
    /// because inlining may change dependency sets.
    if (settings.compile_expressions)
        compileFunctions(actions, output_columns, sample_block, compilation_cache, settings.min_count_to_compile_expression);
#endif

    /// Which columns are needed to perform actions from the current to the last.
    NameSet needed_columns = final_columns;
    /// Which columns nobody will touch from the current action to the last.
    NameSet unmodified_columns;

    {
        NamesAndTypesList sample_columns = sample_block.getNamesAndTypesList();
        for (const auto & name_and_type : sample_columns)
            unmodified_columns.insert(name_and_type.name);
    }

    /// Let's go from the end and maintain set of required columns at this stage.
    /// We will throw out unnecessary actions, although usually they are absent by construction.
    for (int i = static_cast<int>(actions.size()) - 1; i >= 0; --i)
    {
        ExpressionAction & action = actions[i];
        Names in = action.getNeededColumns();

        if (action.type == ExpressionAction::PROJECT)
        {
            needed_columns = NameSet(in.begin(), in.end());
            unmodified_columns.clear();
        }
        else if (action.type == ExpressionAction::ADD_ALIASES)
        {
            needed_columns.insert(in.begin(), in.end());
            for (auto & name : action.projection_aliases)
            {
                auto it = unmodified_columns.find(name);
                if (it != unmodified_columns.end())
                    unmodified_columns.erase(it);
            }
        }
        else if (action.type == ExpressionAction::ARRAY_JOIN)
        {
            /// Do not ARRAY JOIN columns that are not used anymore.
            /// Usually, such columns are not used until ARRAY JOIN, and therefore are ejected further in this function.
            /// We will not remove all the columns so as not to lose the number of rows.
            for (auto it = action.array_joined_columns.begin(); it != action.array_joined_columns.end();)
            {
                bool need = needed_columns.count(it->first);
                if (!need && action.array_joined_columns.size() > 1)
                {
                    action.array_joined_columns.erase(it++);
                }
                else
                {
                    needed_columns.insert(it->first);
                    unmodified_columns.erase(it->first);

                    /// If no ARRAY JOIN results are used, forcibly leave an arbitrary column at the output,
                    ///  so you do not lose the number of rows.
                    if (!need)
                        final_columns.insert(it->first);

                    ++it;
                }
            }
        }
        else
        {
            std::string out = action.result_name;
            if (!out.empty())
            {
                /// If the result is not used and there are no side effects, throw out the action.
                if (!needed_columns.count(out) &&
                    (action.type == ExpressionAction::APPLY_FUNCTION
                    || action.type == ExpressionAction::ADD_COLUMN
                    || action.type == ExpressionAction::COPY_COLUMN))
                {
                    actions.erase(actions.begin() + i);

                    if (unmodified_columns.count(out))
                    {
                        sample_block.erase(out);
                        unmodified_columns.erase(out);
                    }

                    continue;
                }

                unmodified_columns.erase(out);
                needed_columns.erase(out);

                /** If the function is a constant expression, then replace the action by adding a column-constant - result.
                  * That is, we perform constant folding.
                  */
                if (action.type == ExpressionAction::APPLY_FUNCTION && sample_block.has(out))
                {
                    auto & result = sample_block.getByName(out);
                    if (result.column)
                    {
                        action.type = ExpressionAction::ADD_COLUMN;
                        action.result_type = result.type;
                        action.added_column = result.column;
                        action.function_builder = nullptr;
                        action.function_base = nullptr;
                        action.function = nullptr;
                        action.argument_names.clear();
                        in.clear();
                    }
                }
            }

            needed_columns.insert(in.begin(), in.end());
        }
    }

    /// We will not throw out all the input columns, so as not to lose the number of rows in the block.
    if (needed_columns.empty() && !input_columns.empty())
        needed_columns.insert(getSmallestColumn(input_columns));

    /// We will not leave the block empty so as not to lose the number of rows in it.
    if (final_columns.empty() && !input_columns.empty())
        final_columns.insert(getSmallestColumn(input_columns));

    for (auto it = input_columns.begin(); it != input_columns.end();)
    {
        auto it0 = it;
        ++it;
        if (!needed_columns.count(it0->name))
        {
            if (unmodified_columns.count(it0->name))
                sample_block.erase(it0->name);
            input_columns.erase(it0);
        }
    }

/*    std::cerr << "\n";
    for (const auto & action : actions)
        std::cerr << action.toString() << "\n";
    std::cerr << "\n";*/

    /// Deletes unnecessary temporary columns.

    /// If the column after performing the function `refcount = 0`, it can be deleted.
    std::map<String, int> columns_refcount;

    for (const auto & name : final_columns)
        ++columns_refcount[name];

    for (const auto & action : actions)
    {
        if (!action.source_name.name.empty())
            ++columns_refcount[action.source_name];

        for (const auto & name : action.argument_names)
            ++columns_refcount[name];

        for (const auto & name : action.projection_names)
            ++columns_refcount[name];
    }

    Actions new_actions;
    new_actions.reserve(actions.size());

    for (const auto & action : actions)
    {
        new_actions.push_back(action);

        auto process = [&] (const String & name)
        {
            auto refcount = --columns_refcount[name];
            if (refcount <= 0)
            {
                new_actions.push_back(ExpressionAction::removeColumn(name));
                new_actions.back().enumerateColumns(enumerated_columns);
                if (sample_block.has(name))
                    sample_block.erase(name);
            }
        };

        if (!action.source_name.name.empty())
            process(action.source_name);

        for (const auto & name : action.argument_names)
            process(name);

        /// For `projection`, there is no reduction in `refcount`, because the `project` action replaces the names of the columns, in effect, already deleting them under the old names.
    }

    actions.swap(new_actions);

/*    std::cerr << "\n";
    for (const auto & action : actions)
        std::cerr << action.toString() << "\n";
    std::cerr << "\n";*/

    optimizeArrayJoin();
    checkLimits<true>(sample_block, {});
}


std::string ExpressionActions::dumpActions() const
{
    std::stringstream ss;

    ss << "input:\n";
    for (const auto & input_column : input_columns)
        ss << input_column.name << " " << input_column.type->getName() << "\n";

    ss << "\nactions:\n";
    for (auto & action : actions)
        ss << action.toString() << '\n';

    ss << "\noutput:\n";
    NamesAndTypesList output_columns = sample_block.getNamesAndTypesList();
    for (const auto & output_column : output_columns)
        ss << output_column.name << " " << output_column.type->getName() << "\n";

    return ss.str();
}

void ExpressionActions::optimizeArrayJoin()
{
    const size_t NONE = actions.size();
    size_t first_array_join = NONE;

    /// Columns that need to be evaluated for arrayJoin.
    /// Actions for adding them can not be moved to the left of the arrayJoin.
    NameSet array_joined_columns;

    /// Columns needed to evaluate arrayJoin or those that depend on it.
    /// Actions to delete them can not be moved to the left of the arrayJoin.
    NameSet array_join_dependencies;

    for (size_t i = 0; i < actions.size(); ++i)
    {
        /// Do not move the action to the right of the projection (the more that they are not usually there).
        if (actions[i].type == ExpressionAction::PROJECT)
            break;

        bool depends_on_array_join = false;
        Names needed;

        if (actions[i].type == ExpressionAction::ARRAY_JOIN)
        {
            depends_on_array_join = true;
            needed = actions[i].getNeededColumns();
        }
        else
        {
            if (first_array_join == NONE)
                continue;

            needed = actions[i].getNeededColumns();

            for (const auto & column : needed)
            {
                if (array_joined_columns.count(column))
                {
                    depends_on_array_join = true;
                    break;
                }
            }
        }

        if (depends_on_array_join)
        {
            if (first_array_join == NONE)
                first_array_join = i;

            if (!actions[i].result_name.name.empty())
                array_joined_columns.insert(actions[i].result_name);

            for (auto & name : actions[i].array_joined_columns)
                array_joined_columns.insert(name.first);

            array_join_dependencies.insert(needed.begin(), needed.end());
        }
        else
        {
            bool can_move = false;

            if (actions[i].type == ExpressionAction::REMOVE_COLUMN)
            {
                /// If you delete a column that is not needed for arrayJoin (and those who depend on it), you can delete it before arrayJoin.
                can_move = !array_join_dependencies.count(actions[i].source_name);
            }
            else
            {
                /// If the action does not delete the columns and does not depend on the result of arrayJoin, you can make it until arrayJoin.
                can_move = true;
            }

            /// Move the current action to the position just before the first arrayJoin.
            if (can_move)
            {
                /// Move the i-th element to the position `first_array_join`.
                std::rotate(actions.begin() + first_array_join, actions.begin() + i, actions.begin() + i + 1);
                ++first_array_join;
            }
        }
    }
}


BlockInputStreamPtr ExpressionActions::createStreamWithNonJoinedDataIfFullOrRightJoin(const Block & source_header, UInt64 max_block_size) const
{
    for (const auto & action : actions)
        if (action.join && isRightOrFull(action.join->getKind()))
            return action.join->createStreamWithNonJoinedRows(
                source_header, action.join_key_names_left, action.columns_added_by_join, max_block_size);

    return {};
}


/// It is not important to calculate the hash of individual strings or their concatenation
UInt128 ExpressionAction::ActionHash::operator()(const ExpressionAction & action) const
{
    SipHash hash;
    hash.update(action.type);
    hash.update(action.is_function_compiled);
    switch (action.type)
    {
        case ADD_COLUMN:
            hash.update(action.result_name);
            if (action.result_type)
                hash.update(action.result_type->getName());
            if (action.added_column)
                hash.update(action.added_column->getName());
            break;
        case REMOVE_COLUMN:
            hash.update(action.source_name);
            break;
        case COPY_COLUMN:
            hash.update(action.result_name);
            hash.update(action.source_name);
            break;
        case APPLY_FUNCTION:
            hash.update(action.result_name);
            if (action.result_type)
                hash.update(action.result_type->getName());
            if (action.function_base)
            {
                hash.update(action.function_base->getName());
                for (const auto & arg_type : action.function_base->getArgumentTypes())
                    hash.update(arg_type->getName());
            }
            for (const auto & arg_name : action.argument_names)
                hash.update(arg_name);
            break;
        case ARRAY_JOIN:
            hash.update(action.array_join_is_left);
            for (const auto & col : action.array_joined_columns)
                hash.update(col);
            break;
        case JOIN:
            for (const auto & col : action.columns_added_by_join)
                hash.update(col.name);
            break;
        case PROJECT:
            for (const auto & name : action.projection_names)
                hash.update(name);
            for (const auto & name : action.projection_aliases)
                hash.update(name);
            break;
        case ADD_ALIASES:
            break;
    }
    UInt128 result;
    hash.get128(result.low, result.high);
    return result;
}

bool ExpressionAction::operator==(const ExpressionAction & other) const
{
    if (result_type != other.result_type)
    {
        if (result_type == nullptr || other.result_type == nullptr)
            return false;
        else if (!result_type->equals(*other.result_type))
            return false;
    }

    if (function_base != other.function_base)
    {
        if (function_base == nullptr || other.function_base == nullptr)
            return false;
        else if (function_base->getName() != other.function_base->getName())
            return false;

        const auto & my_arg_types = function_base->getArgumentTypes();
        const auto & other_arg_types = other.function_base->getArgumentTypes();
        if (my_arg_types.size() != other_arg_types.size())
            return false;

        for (size_t i = 0; i < my_arg_types.size(); ++i)
            if (!my_arg_types[i]->equals(*other_arg_types[i]))
                return false;
    }

    if (added_column != other.added_column)
    {
        if (added_column == nullptr || other.added_column == nullptr)
            return false;
        else if (added_column->getName() != other.added_column->getName())
            return false;
    }

    return source_name == other.source_name
        && result_name == other.result_name
        && argument_names == other.argument_names
        && array_joined_columns == other.array_joined_columns
        && array_join_is_left == other.array_join_is_left
        && join == other.join
        && join_key_names_left == other.join_key_names_left
        && columns_added_by_join == other.columns_added_by_join
        && projection_names == other.projection_names
        && projection_aliases == other.projection_aliases
        && is_function_compiled == other.is_function_compiled;
}

void ExpressionActionsChain::addStep()
{
    if (steps.empty())
        throw Exception("Cannot add action to empty ExpressionActionsChain", ErrorCodes::LOGICAL_ERROR);

    ColumnsWithTypeAndName columns = steps.back().actions->getSampleBlock().getColumnsWithTypeAndName();
    steps.push_back(Step(std::make_shared<ExpressionActions>(columns, context)));
}

void ExpressionActionsChain::finalize()
{
    /// Finalize all steps. Right to left to define unnecessary input columns.
    for (int i = static_cast<int>(steps.size()) - 1; i >= 0; --i)
    {
        Names required_output = steps[i].required_output;
        std::unordered_map<String, size_t> required_output_indexes;
        for (size_t j = 0; j < required_output.size(); ++j)
            required_output_indexes[required_output[j]] = j;
        auto & can_remove_required_output = steps[i].can_remove_required_output;

        if (i + 1 < static_cast<int>(steps.size()))
        {
            const NameSet & additional_input = steps[i + 1].additional_input;
            for (const auto & it : steps[i + 1].actions->getRequiredColumnsWithTypes())
            {
                if (additional_input.count(it.name) == 0)
                {
                    auto iter = required_output_indexes.find(it.name);
                    if (iter == required_output_indexes.end())
                        required_output.push_back(it.name);
                    else if (!can_remove_required_output.empty())
                        can_remove_required_output[iter->second] = false;
                }
            }
        }
        steps[i].actions->finalize(required_output);
    }

    /// When possible, move the ARRAY JOIN from earlier steps to later steps.
    for (size_t i = 1; i < steps.size(); ++i)
    {
        ExpressionAction action;
        if (steps[i - 1].actions->popUnusedArrayJoin(steps[i - 1].required_output, action))
            steps[i].actions->prependArrayJoin(action, steps[i - 1].actions->getSampleBlock());
    }

    /// Adding the ejection of unnecessary columns to the beginning of each step.
    for (size_t i = 1; i < steps.size(); ++i)
    {
        size_t columns_from_previous = steps[i - 1].actions->getSampleBlock().columns();

        /// If unnecessary columns are formed at the output of the previous step, we'll add them to the beginning of this step.
        /// Except when we drop all the columns and lose the number of rows in the block.
        if (!steps[i].actions->getRequiredColumnsWithTypes().empty()
            && columns_from_previous > steps[i].actions->getRequiredColumnsWithTypes().size())
            steps[i].actions->prependProjectInput();
    }
}

std::string ExpressionActionsChain::dumpChain()
{
    std::stringstream ss;

    for (size_t i = 0; i < steps.size(); ++i)
    {
        ss << "step " << i << "\n";
        ss << "required output:\n";
        for (const std::string & name : steps[i].required_output)
            ss << name << "\n";
        ss << "\n" << steps[i].actions->dumpActions() << "\n";
    }

    return ss.str();
}

}
