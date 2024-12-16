#include "PartitionOutputFormat.h"
#include "Common/ArenaUtils.h"
#include <Common/Exception.h>

#include <boost/algorithm/string/join.hpp>
#include <ranges>
#include <unordered_map>
#include <boost/range/adaptor/map.hpp>


namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace DB {

const std::string PARTITION_ID_WILDCARD = "_partition_id";

String formatTemplate(const String & out_file_template, const PartitionOutputFormat::Key & key, const std::unordered_map<String, int>& key_name_to_index) {
    String res;
    std::vector<bool> used(key.size());
    int n = static_cast<int>(out_file_template.size());

    for (int i = 0; i < n; ++i) {
        char x = out_file_template[i];

        if (x == '\\' && i + 1 < n) {
            if (out_file_template[i + 1] == '{') {
                ++i;
                res.push_back('{');
                continue;
            }
            if (out_file_template[i + 1] == '}') {
                ++i;
                res.push_back('}');
                continue;
            }
        }
        if (x == '}') {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not escaped '}}' in out_file_template at pos {}. Escape it using backslash '\'", i);
        }
        if (x != '{') {
            res.push_back(x);
            continue;
        }

        std::string name;
        bool found = false;
        for (int j = i + 1; j < n; ++j) {
            x = out_file_template[j];
            if (x == '\\' && j + 1 < n) {
                if (out_file_template[j + 1] == '{') {
                    ++j;
                    name.push_back('{');
                    continue;
                }
                if (out_file_template[j + 1] == '}') {
                    ++j;
                    name.push_back('}');
                    continue;
                }
            }
            if (x == '{') {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not escaped '{{' inside out_file_template at pos {}. Escape it using backslash '\'", j);
            }
            if (x != '}') {
                name.push_back(x);
                continue;
            }
            found = true;
            int key_index;
            auto it = key_name_to_index.find(name);
            if (it == key_name_to_index.end()) {
                if (name != PARTITION_ID_WILDCARD)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected column name in out_file_template: {}", name);
                if (key.size() != 1)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected column name in out_file_template: {}. Can only use {{_partition_id}} with one key", name);
                key_index = 0;
            } else {
                key_index = it->second;
            }

            used[key_index] = true;
            res.append(key[key_index].toView());
            i = j;
            name.clear();
            break;
        }
        if (!found) {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No matching '}}' for '{{' in out_file_template at pos {}", i);
        }
    }
    if (!std::all_of(begin(used), end(used), std::identity{})) {
        auto missed_columns_view = key_name_to_index
            | std::views::filter([&](const auto & pair) { return !used[pair.second];})
            | std::views::transform([](const auto & pair) { return pair.first; });
        std::vector<std::string> missed_columns(missed_columns_view.begin(), missed_columns_view.end());
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missed columns in out_file_template: {}. Must use all of them", boost::algorithm::join(missed_columns, ", "));
    }
    return res;
}

const ASTs & getChildernsInPartitionBy(const ASTPtr & expr_list) {
    if (expr_list->children.size() != 1) {
        return expr_list->children;
    }
    const auto * func = expr_list->children.front()->as<ASTFunction>();
    if (!func || func->name != "tuple") {
        return expr_list->children;
    }
    return func->arguments->children;
}

void throwIfTemplateIsNotValid(const String & out_file_template, const ASTPtr & partition_by)
{
    std::unordered_map<String, int> partition_key_name_to_index;
    PartitionOutputFormat::Key key;
    int i = 0;
    for (const ASTPtr & expr : getChildernsInPartitionBy(partition_by))
    {
        partition_key_name_to_index.emplace(expr->getAliasOrColumnName(), i++);
        key.push_back("");
    }

    formatTemplate(out_file_template, key, partition_key_name_to_index);
}

PartitionOutputFormat::PartitionOutputFormat(
    const InternalFormatterCreator & internal_formatter_creator_,
    WriteBuffer & fake_buffer,
    const Block & header_,
    const String & out_file_template_,
    const ASTPtr & partition_by,
    const ContextPtr & context)
    : IOutputFormat(header_, fake_buffer), header(header_), out_file_template(out_file_template_), internal_formatter_creator(internal_formatter_creator_)
{
    int i = 0;
    for (const ASTPtr & expr : getChildernsInPartitionBy(partition_by))
    {
        partition_key_name_to_index.emplace(expr->getAliasOrColumnName(), i++);

        ASTs arguments(1, expr);
        ASTPtr partition_by_string = makeASTFunction("toString", std::move(arguments));
        partition_by_expr_names.push_back(partition_by_string->getColumnName());

        auto syntax_result = TreeRewriter(context).analyze(partition_by_string, header.getNamesAndTypesList());
        partition_by_exprs.push_back(ExpressionAnalyzer(partition_by_string, syntax_result, context).getActions(false));
    }
}

void PartitionOutputFormat::consume(Chunk chunk)
{
    Columns key_columns;
    const auto & columns = chunk.getColumns();
    for (int i = 0; i < static_cast<int>(partition_by_exprs.size()); ++i)
    {
        Block block_with_partition_by_expr = header.cloneWithoutColumns();
        block_with_partition_by_expr.setColumns(columns);
        // need allow_duplicates_in_input?
        partition_by_exprs[i]->execute(block_with_partition_by_expr);
        key_columns.push_back(std::move(block_with_partition_by_expr.getByName(partition_by_expr_names[i]).column));
    }

    std::unordered_map<Key, size_t, KeyHash> key_to_chunk_index;
    IColumn::Selector selector;
    for (size_t row = 0; row < chunk.getNumRows(); ++row)
    {
        Key key;
        key.reserve(key_columns.size());
        for (auto & key_column : key_columns)
        {
            key.push_back(key_column->getDataAt(row));
        }
        auto [it, _] = key_to_chunk_index.emplace(key, key_to_chunk_index.size());
        selector.push_back(it->second);
    }

    Chunks sub_chunks;
    sub_chunks.reserve(key_to_chunk_index.size());
    for (size_t column_index = 0; column_index < columns.size(); ++column_index)
    {
        MutableColumns column_sub_chunks = columns[column_index]->scatter(key_to_chunk_index.size(), selector);
        if (column_index == 0) /// Set sizes for sub-chunks.
        {
            for (const auto & column_sub_chunk : column_sub_chunks)
            {
                sub_chunks.emplace_back(Columns(), column_sub_chunk->size());
            }
        }
        for (size_t sub_chunk_index = 0; sub_chunk_index < column_sub_chunks.size(); ++sub_chunk_index)
        {
            sub_chunks[sub_chunk_index].addColumn(std::move(column_sub_chunks[sub_chunk_index]));
        }
    }

    for (const auto & [partition_key, index] : key_to_chunk_index)
    {
        getOrCreateOutputFormat(partition_key)->write(header.cloneWithColumns(sub_chunks[index].detachColumns()));
    }
}

OutputFormatPtr PartitionOutputFormat::getOrCreateOutputFormat(const Key & key)
{
    auto it = partition_key_to_output_format.find(key);
    if (it == partition_key_to_output_format.end())
    {
        auto filepath = formatTemplate(out_file_template, key, partition_key_name_to_index);
        auto output_format = internal_formatter_creator(filepath);
        std::tie(it, std::ignore) = partition_key_to_output_format.emplace(copyKeyToArena(key), output_format);
    }
    return it->second;
}

PartitionOutputFormat::Key PartitionOutputFormat::copyKeyToArena(const Key & key)
{
    std::vector<StringRef> res;
    res.reserve(key.size());

    for (const StringRef & part : key) {
        res.push_back(copyStringInArena(partition_keys_arena, part));
    }
    return res;
}

void PartitionOutputFormat::finalizeImpl()
{
    for (auto & [_, output_format] : partition_key_to_output_format)
    {
        output_format->finalize();
    }
}
}
