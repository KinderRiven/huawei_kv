#pragma once
#include <memory>
#include <mutex>
#include <map>

#include "config.h"
#include "kv_string.h"
#include "kv_intf.h"
#include "data_agent.h"

class KVService : public KVIntf, public std::enable_shared_from_this<KVService>
{
public:
  KVService();
  ~KVService();

public:
  bool Init(const char *host, int id);
  void Close();

public:
  int Set(KVString &key, KVString &val);
  int Get(KVString &key, KVString &val);

  std::shared_ptr<KVService> GetPtr()
  {
    return shared_from_this();
  }

public:
  bool init;
  int connect_count;
  int partition_id;
  std::mutex mutex_;

public:
  std::mutex agent_mutex[NUM_AGENT];
  DataAgent *agent[NUM_AGENT];

public:
  std::map<uint64_t, int> write_thread_map;
  std::map<uint64_t, int> read_thread_map;
};
