#pragma once

#include <cstdint>
#include <map>
#include <memory>

#include "kv_string.h"
#include "config.h"
#include "PFHT.h"
#include "hashkv.h"
#include "utils.h"
#include "cache.h"

class DataAgent
{
public:
  DataAgent(const char url[]);
  ~DataAgent();

public:
  int Init();
  int Connect(const char url[]);
  void Release();
  void Clear();

public:
  int Append(KVString &key, KVString &val);
  int Get(KVString &key, KVString &val);
  int GetMetaData();
  int GetIndexData();

public:
  char url_[64];
  int fd_;
  uint64_t offset_;

public:
  FIFOCache *cache_;
  PFHT *index_;
  Packet *packet;
  std::map<uint64_t, uint8_t> hot_;

public:
  uint64_t recv_cache_count_;
  uint64_t read_cache_count_;
  uint64_t read_opt_count_;
  uint64_t write_opt_count_;
};
