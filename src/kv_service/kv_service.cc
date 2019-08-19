#include "kv_service.h"
#include "utils.h"
#include "config.h"

#include <thread>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <pthread.h>

KVService::KVService()
{
    LOG(INFO) << "[KVService::KVService()] A service ready to run !";
    init = false;
    partition_id = 0;
}

KVService::~KVService()
{
    LOG(INFO) << "[KVService::~KVService()] A service ready to exited!";
    mutex_.lock();
    if (init)
    {
        init = false;
        for (int i = 0; i < NUM_AGENT; i++)
        {
            agent[i]->Release();
        }
    }
    mutex_.unlock();
}

/*
    In order to increase concurrency, we use multiple agents. 
    port base is (9527++).
*/
bool KVService::Init(const char *host, int id)
{
    LOG(INFO) << "[KVService::Init()] Init service! (" << id << ")";
    mutex_.lock();
    connect_count++;

    if (!init)
    {
        char host_[64];
        partition_id = 0;
        init = true;
        for (int i = 0; i < NUM_AGENT; i++)
        {
            snprintf(host_, sizeof(host_), "%s:%d", host, SERVER_START_PORT + i);
            agent[i] = new DataAgent(host_);
            agent[i]->Init();
        }
    }
    mutex_.unlock();
    return true;
}

/*
    Free all source, only one thread can call this function.
    But we don't release connect here.
 */
void KVService::Close()
{
    mutex_.lock();
    LOG(INFO) << "[KVService::Init()] Close!";
    connect_count--;
    assert(connect_count >= 0);
    if (connect_count == 0)
    {
        write_thread_map.clear();
        read_thread_map.clear();
    }
    mutex_.unlock();
}

/*
    Distribute data to different ports based on key-value distribution.
 */
int KVService::Set(KVString &key, KVString &val)
{
    mutex_.lock();
    int agent_id;
    uint64_t prefix = (uint64_t)pthread_self();
    auto find = write_thread_map.find(prefix);

    if (find == write_thread_map.end())
    {
        LOG(INFO) << "[KVService::Set] Create new write_thread_map[" << prefix << "]=" << partition_id << ".";
        write_thread_map.insert(std::pair<uint64_t, int>(prefix, partition_id));
        partition_id++;
    }

    agent_id = write_thread_map[prefix] % NUM_AGENT;
    mutex_.unlock();

    agent_mutex[agent_id].lock();
    int ret = agent[agent_id]->Append(key, val);
    agent_mutex[agent_id].unlock();
    return ret;
}

/*
    Distribute data to different ports based on key-value distribution.
 */
int KVService::Get(KVString &key, KVString &val)
{
    mutex_.lock();
    int agent_id;
    uint64_t prefix = (uint64_t)pthread_self();
    auto find = read_thread_map.find(prefix);

    if (find == read_thread_map.end())
    {
        bool ok = false;
        for (int i = 0; i < NUM_AGENT; i++)
        {
            agent_mutex[i].lock();
            int ret = agent[i]->Get(key, val);
            agent_mutex[i].unlock();
            if (ret)
            {
                ok = true;
                read_thread_map.insert(std::pair<uint64_t, int>(prefix, i));
                LOG(INFO) << "[KVService::Get] Bind new read_thread_map[" << prefix << "]=" << i << ".";
                break;
            }
        }
        if (!ok)
        {
            LOG(INFO) << "[KVService::Get] Can't establish read_thread_map[" << prefix << "]";
        }
    }

    agent_id = read_thread_map[prefix];
    mutex_.unlock();

    agent_mutex[agent_id].lock();
    int ret = agent[agent_id]->Get(key, val);
    agent_mutex[agent_id].unlock();
    return ret;
}
