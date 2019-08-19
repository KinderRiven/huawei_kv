#ifndef __HUAWEI_UTILS_H__
#define __HUAWEI_UTILS_H__
//////////////////////////////////////////////////////////////////////////////////////////////////
#include "easylogging++.h"

#define KV_LOG(level) LOG(level) << "[" << __FUNCTION__ << ":" << __LINE__ << "] "

const int KV_OP_META_APPEND = 1;
const int KV_OP_META_GET = 2;
const int KV_OP_DATA_APPEND = 3;
const int KV_OP_DATA_GET = 4;
const int KV_OP_CLEAR = 5;

#pragma pack(push)
#pragma pack(1)
const int PACKET_HEADER_SIZE = sizeof(int32_t) * 4;

#include <stdint.h>

#define GET_KV_LENGTH(key_length, value_length) ((((uint64_t)key_length << 32) & 0xffffffff00000000) | ((uint64_t)value_length & 0x00000000ffffffff))
#define GET_KEY_LENGTH(kv_length) (((uint64_t)kv_length >> 32) & 0x00000000ffffffff)
#define GET_VALUE_LENGTH(kv_length) (((uint64_t)kv_length) & 0x00000000ffffffff)

#define KV_OPT_PUT (1)
#define KV_OPT_GET (2)
#define KV_OPT_CLOSE (3)
#define KV_OPT_INDEX (4)
#define KV_OPT_METEDATA (5)
#define KV_OPT_CACHE (6)

struct packet_header
{
public:
  uint64_t opt;        // packet type (8B)
  uint64_t file_id;    // file_id for data (8B)
  uint64_t offset;     // write offset (8B)
  uint64_t total_size; // total size
};

class Packet
{
public:
  Packet()
  {
  }

public:
  static Packet *GetNewPacket(size_t buffer_size)
  {
    Packet *p = (Packet *)(new char[sizeof(struct packet_header) + buffer_size]);
    p->header.total_size = sizeof(struct packet_header) + buffer_size;
    p->header.offset = 0;
    return p;
  }

  static void DeletePacket(Packet *packet)
  {
    char *d_ = (char *)packet;
    delete[] d_;
  }

public:
  struct packet_header header; // packet header
  char buf_[0];                // data buffer
};

#pragma pack(pop)

//////////////////////////////////////////////////////////////////////////////////////////////////
#endif
