#ifndef INCLUDE_CACHE_H_
#define INCLUDE_CACHE_H_

#include <mutex>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <fcntl.h>

#include "config.h"

// 4 * 16MB = 64MB (cache)
#define HOT_PARAM (10)
#define NUM_CACHE_SLOT (4)
#define CACHE_NODE_SIZE (8 * 1024 * 1024)

#define CACHE_ALIGN(offset) ((uint64_t)(offset / CACHE_NODE_SIZE) * CACHE_NODE_SIZE)

struct cache_node
{
  public:
    uint64_t offset;
    uint64_t size;
    char buf_[CACHE_NODE_SIZE];

  public:
    cache_node()
    {
        offset = 0;
        size = 0;
    }
};

class FIFOCache
{
  public:
    FIFOCache()
    {
        for (int i = 0; i < NUM_CACHE_SLOT; i++)
        {
            node[i] = NULL;
        }
        cache_miss = 0;
        cache_hit = 0;
        cache_pretch = 0;
    }

    ~FIFOCache()
    {
        Clear();
    }

    void Clear()
    {
        for (int i = 0; i < NUM_CACHE_SLOT; i++)
        {
            if (node[i] != NULL)
            {
                delete node[i];
                node[i] = NULL;
            }
        }
        cache_miss = 0;
        cache_hit = 0;
        cache_pretch = 0;
    }

    int ReadKV(uint64_t offset, char *buf_)
    {
        uint64_t align = CACHE_ALIGN(offset);
        assert(align % CACHE_NODE_SIZE == 0);

        for (int i = 0; i < NUM_CACHE_SLOT; i++)
        {
            if (node[i] != NULL && node[i]->offset == align)
            {
                if ((offset - align) < node[i]->size)
                {
                    memcpy(buf_, node[i]->buf_ + (offset - align), VALUE_LENGTH); // succeed
                    cache_hit++;
                    return i;
                }
            }
        }

        cache_miss++;
        return -1;
    }

    int ReadBlock(uint64_t offset, char *buf_)
    {
        uint64_t align = CACHE_ALIGN(offset);
        assert(align % CACHE_NODE_SIZE == 0);

        for (int i = 0; i < NUM_CACHE_SLOT; i++)
        {
            if (node[i] != NULL && node[i]->offset == align)
            {
                if ((offset - align) < node[i]->size)
                {
                    memcpy(buf_, node[i], sizeof(struct cache_node));
                    return i;
                }
            }
        }

        return -1;
    }

    // FIFO Algorithm()
    // Delete first cache block
    void Append(struct cache_node *new_node)
    {
        struct cache_node *garbage = node[0];

        for (int i = 0; i < NUM_CACHE_SLOT - 1; i++)
        {
            node[i] = node[i + 1];
        }

        node[NUM_CACHE_SLOT - 1] = new_node;

        if (garbage != NULL)
        {
            delete garbage;
        }
    }

    void Print()
    {
        uint64_t memory_usage = (uint64_t)CACHE_NODE_SIZE * NUM_CACHE_SLOT / (1024 * 1024);
        LOG(INFO) << "[LRUCache::Print] cache_miss/cache_hit/cache_pretch:" << cache_miss << "/" << cache_hit << "/" << cache_pretch;
        LOG(INFO) << "[LRUCache::Print] memory used:" << memory_usage << "MB";
    }

  public:
    struct cache_node *node[NUM_CACHE_SLOT];
    uint64_t cache_miss;
    uint64_t cache_hit;
    uint64_t cache_pretch = 0;
};

#endif