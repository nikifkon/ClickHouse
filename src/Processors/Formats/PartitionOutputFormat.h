#pragma once

#include <unordered_map>
#include <Common/Arena.h>
#include <Processors/Formats/IOutputFormat.h>

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>

#include <Parsers/IAST_fwd.h>
#include <Common/ThreadPool.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/Event.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/NullWriteBuffer.h>
#include <Common/SipHash.h>
#include <Columns/IColumn.h>


namespace DB
{

void throwIfPatternIsNotValid(const String & pattern, const ASTPtr & partition_by);

using OutputFormatPtr = std::shared_ptr<IOutputFormat>;

class PartitionOutputFormat : public IOutputFormat {
public:
    using InternalFormatterCreator = std::function<OutputFormatPtr(const String& name)>;

    using Key = StringRefs;

    struct KeyHash
    {
        size_t operator()(const StringRefs & key) const
        {
            SipHash hash;
            hash.update(key.size());
            for (const auto & part : key)
                hash.update(part.toView());
            return hash.get64();
        }
    };

    PartitionOutputFormat(
        const InternalFormatterCreator & internal_formatter_creator_,
        WriteBuffer & fake_buffer,
        const Block & header_,
        const String & pattern_,
        const ASTPtr & partition_by,
        const ContextPtr & context);

    String getName() const override { return "PartitionOutputFormat"; }

protected:
    void consume(Chunk chunk) override;

private:
    OutputFormatPtr getOrCreateOutputFormat(const Key & key);
    Key copyKeyToArena(const Key & key);

    std::unordered_map<Key, OutputFormatPtr, KeyHash> partition_key_to_output_format;
    std::unordered_map<String, int> partition_key_name_to_index;
    Arena partition_keys_arena;

    std::vector<ExpressionActionsPtr> partition_by_exprs;
    std::vector<String> partition_by_expr_names;

    Block header;
    String pattern;
    InternalFormatterCreator internal_formatter_creator;
};

};
