#include "DynamicForkWriteBuffer.h"


namespace DB
{


DynamicForkWriteBuffer::DynamicForkWriteBuffer() : WriteBuffer(nullptr, 0)
{
}

DynamicForkWriteBuffer::~DynamicForkWriteBuffer() {
    // see why its not recommened
    finalize();
}

void DynamicForkWriteBuffer::addWriteBuffer(WriteBufferPtr && buffer)
{
    sources.push_back(std::move(buffer));
}

void DynamicForkWriteBuffer::nextImpl()
{
    for (const WriteBufferPtr & buffer : sources)
    {
        buffer->next();
    }
}
void DynamicForkWriteBuffer::finalizeImpl()
{
    for (const WriteBufferPtr & buffer : sources)
    {
        buffer->finalize();
    }
}
}
