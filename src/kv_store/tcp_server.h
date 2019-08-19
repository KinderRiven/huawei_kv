#ifndef __HUAWEI_TCP_SERER__
#define __HUAWEI_TCP_SERER__

#include <map>
#include <mutex>
#include <memory>
#include <vector>

#include "hashkv.h"
#include "utils.h"

class TcpServer
{
public:
  TcpServer();
  ~TcpServer();

public:
  int Init(int id, const char dir[], const char url[]);
  int Connect(const char url[]);

public:
  void Start();
  void Stop();

public:
  int GetKV(Packet *packet);
  int SetKV(Packet *packet);
  int GetCacheBlock(Packet *packet);
  int GetMetaData();
  int GetIndexData();
  int Print();

private:
  int fd_;
  KVStore *store_;
  std::map<uint64_t, uint8_t> hot_;

private:
  Packet *rep_packet;
  char *buf_;
  char *value_buf_;
  char *key_buf_;

private:
  uint64_t write_opt_count_;
  uint64_t read_opt_count_;
  uint64_t cache_opt_count_;
};

#endif
