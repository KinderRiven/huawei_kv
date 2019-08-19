#include "hashkv.h"
#include "file_tool.h"

#include <errno.h>
#include <dirent.h>
#include <iostream>
#include <assert.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h>

Log::Log(bool direct_io, const char *path)
{
    strcpy(this->path, path);
    this->direct_io = direct_io;

    if (direct_io)
    {
        fd = open(this->path, O_RDWR | O_CREAT | O_DIRECT, 0777);
        offset = lseek(fd, 0, SEEK_END);
        LOG(INFO) << "[KVStore::Log][DIRECT_IO][Path:" << this->path << "][offset:" << offset << "]";
    }
    else
    {
        fd = open(this->path, O_RDWR | O_CREAT, 0777);
        offset = lseek(fd, 0, SEEK_END);
        LOG(INFO) << "[KVStore::Log][WO_DIRECT_IO][Path:" << this->path << "][offset:" << offset << "]";
    }

    if (fd == -1)
    {
        LOG(INFO) << "[Log::Log] Open file failed!";
    }
}

Log::~Log()
{
    close(fd);
}

uint64_t Log::Append(const char *buf_, size_t size_)
{
    uint64_t ret = offset;
    write(fd, buf_, size_);
    offset += size_;
    return ret;
}

uint64_t Log::Read(uint64_t offset, char *buf_, size_t buffer_size)
{
    uint64_t size = pread(fd, buf_, buffer_size, offset);
    return size;
}

KVStore::KVStore()
{
}

KVStore::~KVStore()
{
    Close();
}

bool KVStore::Init(const char *dir, int id)
{
    num_kv = 0;
    thread_id = id;

    snprintf(home_path, sizeof(home_path), "%s_%d", dir, id);
    mkdir(home_path, 0777);

    char path[64];
    snprintf(path, sizeof(path), "%s/data", home_path);
    value_store = new Log(false, path); // use buffer
    snprintf(path, sizeof(path), "%s/index", home_path);
    key_store = new Log(false, path); // use buffer

    value_cache = new FIFOCache();
    return true;
}

void KVStore::RecoveryIndex(char *partition, char *extra_partition)
{
    LOG(INFO) << "[KVStore::RecoveryIndex] Recovery index (" << thread_id << ").";
    size_t read_size = 0;
    uint64_t offset = 0;

    char data_path[64];
    char read_buffer[KEY_LENGTH];
    sprintf(data_path, "%s/index", home_path);

    PFHT *index = new PFHT();
    index->partition = (struct ht_bucket *)partition;
    index->extra_partition = (struct ht_bucket *)extra_partition;

    int fd = open(data_path, O_RDWR, 0777);

    if (fd == -1)
    {
        LOG(ERROR) << "[KVStore::RecoveryIndex] Recovery index error (1)!";
    }

    LOG(INFO) << "[KVStore::RecoveryIndex] Recovery index (ing)...";

    while (true)
    {
        read_size = read(fd, read_buffer, KEY_LENGTH);
        if (read_size != KEY_LENGTH)
        {
            break;
        }
        bool res = index->insert((uint8_t *)read_buffer, offset);
        if (res == false)
        {
            LOG(ERROR) << "[KVStore::RecoveryIndex] Recovery index error (2)!";
        }
        offset += VALUE_LENGTH;
        num_kv++;
    }

    LOG(INFO) << "[KVStore::RecoveryIndex] Recovery index (ed)...";
    index->partition = NULL;
    index->extra_partition = NULL;
    index->buf_ = NULL;
    delete index;
    close(fd);
}

void KVStore::Close()
{
    if (key_store != NULL)
    {
        delete key_store;
        key_store = NULL;
    }

    if (value_store != NULL)
    {
        delete value_store;
        value_store = NULL;
    }
}

int KVStore::Set(char *key, char *val)
{
    key_store->Append(key, KEY_LENGTH);     // write key
    value_store->Append(val, VALUE_LENGTH); // write value
    return 0;
}

int KVStore::Get(uint64_t offset, char *val)
{
    value_store->Read(offset, val, VALUE_LENGTH);
    return 0;
}

/*
int KVStore::Get(uint64_t offset, char *val)
{
    int ret = value_cache->ReadKV(offset, val);

    if (ret != -1)
    {
        return ret; // block index
    }
    else
    {
        value_store->Read(offset, val, VALUE_LENGTH); // direct read without block
        return -1;
    }
}
*/

int KVStore::GetCacheBlock(uint64_t offset, char *buf)
{
    int ret = value_cache->ReadBlock(offset, buf);

    if (ret == -1) // error need to read from SSD
    {
        cache_node *node = new cache_node();
        node->offset = CACHE_ALIGN(offset);
        node->size = value_store->Read(node->offset, node->buf_, CACHE_NODE_SIZE);
        value_cache->Append(node);
        memcpy(buf, node, sizeof(struct cache_node));
    }

    return ret;
}

int KVStore::PrefGetCacheBlock()
{
    if (value_cache->node[0] == NULL)
    {
        return 0;
    }

    int move = value_cache->node[NUM_CACHE_SLOT - 1]->offset - value_cache->node[NUM_CACHE_SLOT - 2]->offset;

    uint64_t offset = 0;

    if (move > 0)
    {
        offset = value_cache->node[NUM_CACHE_SLOT - 1]->offset + CACHE_NODE_SIZE; // front to behind
    }
    else
    {
        offset = value_cache->node[NUM_CACHE_SLOT - 1]->offset - CACHE_NODE_SIZE; // behind to front
    }

    cache_node *node = new cache_node();
    node->offset = CACHE_ALIGN(offset);
    node->size = value_store->Read(node->offset, node->buf_, CACHE_NODE_SIZE);
    value_cache->Append(node);
    return 1;
}

int KVStore::AppendNewBlock(uint64_t offset)
{
    cache_node *node = new cache_node();
    node->offset = CACHE_ALIGN(offset);
    node->size = value_store->Read(node->offset, node->buf_, CACHE_NODE_SIZE);
    value_cache->Append(node);
    return 1;
}

uint64_t KVStore::Read(uint64_t offset, char *buf_, size_t buffer_size)
{
    return 0;
}

void KVStore::Print()
{
    value_cache->Print();
}