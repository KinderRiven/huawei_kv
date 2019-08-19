#pragma once
#include <memory>

#include <map>
#include <string>
#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/time.h>

#include "PFHT.h"
#include "config.h"
#include "cache.h"
#include "kv_string.h"

class Log
{
public:
  Log(bool direct_io, const char *path);
  ~Log();

public:
  uint64_t Append(const char *buf, size_t size);
  uint64_t Read(uint64_t offset, char *buf_, size_t buffer_size);

public:
  bool direct_io;  // use direct IO
  char path[64];   // log path
  int fd;          // no direct IO fd
  uint64_t offset; // write offset
};

class KVStore
{
public:
  KVStore();
  ~KVStore();

public:
  bool Init(const char *dir, int id);
  void Close();
  void Print();

public:
  int Set(char *key, char *val);
  int Get(uint64_t offset, char *val);
  uint64_t Read(uint64_t offset, char *buf_, size_t buffer_size); // read data from offset
  int UpdateCache(uint64_t offset);
  int AppendNewBlock(uint64_t offset);
  int GetCacheBlock(uint64_t offset, char *buf);
  int PrefGetCacheBlock();

public:
  void RecoveryIndex(char *partition, char *extra_partition);

public:
  Log *value_store;
  Log *key_store;
  FIFOCache *value_cache;

public:
  char home_path[64];
  int thread_id;

public:
  uint64_t num_kv;
};
