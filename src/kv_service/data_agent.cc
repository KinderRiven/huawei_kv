#include "data_agent.h"
#include "utils.h"
#include "config.h"
#include "PFHT.h"
#include "nanomsg/nn.h"
#include "nanomsg/pair.h"

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

DataAgent::DataAgent(const char url[])
{
    fd_ = -1;
    strcpy(url_, url);
    offset_ = 0;
    cache_ = new FIFOCache();
    packet = Packet::GetNewPacket(KEY_LENGTH + VALUE_LENGTH);

    recv_cache_count_ = 0;
    read_cache_count_ = 0;
    read_opt_count_ = 0;
    write_opt_count_ = 0;
}

DataAgent::~DataAgent()
{
    Release();
}

int DataAgent::Init()
{
    index_ = new PFHT();
    index_->init();

    fd_ = Connect(url_);

    if (fd_ == -1)
    {
        LOG(INFO) << "[DataAgent::Init] Connect failed while init and waiting to re-connect!";
        return 0;
    }

    GetMetaData();
    GetIndexData();

    return 1;
}

int DataAgent::Connect(const char url[])
{
    if (fd_ != -1)
    {
        nn_close(fd_);
    }

    int fd = nn_socket(AF_SP, NN_PAIR);

    if (fd < 0)
    {
        return -1;
    }

    int time1 = 500;
    int time2 = 500;
    nn_setsockopt(fd, 0, NN_SNDTIMEO, &time1, sizeof(time1));
    nn_setsockopt(fd, 0, NN_RCVTIMEO, &time2, sizeof(time2));

    if (nn_connect(fd, url) < 0)
    {
        nn_close(fd);
        return -1;
    }
    return fd;
}

void DataAgent::Release()
{
    LOG(INFO) << "Recv/Read Cache Count : " << recv_cache_count_ << "/" << read_cache_count_;

    if (index_ != NULL)
    {
        delete index_;
        index_ = NULL;
    }
    if (fd_ != -1)
    {
        LOG(INFO) << "[DataAgent::Release] Close socket (" << fd_ << ")";
        Packet *packet = Packet::GetNewPacket(0);
        packet->header.opt = KV_OPT_CLOSE;
        nn_send(fd_, packet, packet->header.total_size, 0);
        nn_recv(fd_, packet, packet->header.total_size, 0);
        nn_close(fd_);
        fd_ = -1;
    }
    if (cache_ != NULL)
    {
        delete cache_;
        cache_ = NULL;
    }

    Packet::DeletePacket(packet);
    recv_cache_count_ = 0;
    read_cache_count_ = 0;
    read_opt_count_ = 0;
    write_opt_count_ = 0;
}

void DataAgent::Clear()
{
}

int DataAgent::GetMetaData()
{
    int size;
    packet->header.opt = KV_OPT_METEDATA;
    packet->header.total_size = sizeof(struct packet_header);
    size = nn_send(fd_, packet, packet->header.total_size, 0);

    if (size != sizeof(struct packet_header))
    {
        LOG(ERROR) << "[DataAgent::Init] Init failed while get metedata (1) ! " << nn_strerror(errno);
        return 0;
    }

    LOG(INFO) << "[DataAgent::Init] Waiting receive metedata!";
    size = nn_recv(fd_, packet, sizeof(struct packet_header), 0);

    if (size != sizeof(struct packet_header))
    {
        LOG(ERROR) << "[DataAgent::Init] Init failed while get metedata (2) ! " << nn_strerror(errno);
        return 0;
    }

    offset_ = packet->header.offset;
    LOG(INFO) << "[DataAgent::Init] Get metedata finished!";
    return 1;
}

int DataAgent::GetIndexData()
{
    if (offset_ == 0)
    {
        LOG(INFO) << "[DataAgent::Init] NewDB need not to get index data! (" << url_ << ")";
        return 1;
    }

    size_t bucket_size = (TOTAL_NUM_BUCKET + TOTAL_NUM_EXTRA_BUCKET) * sizeof(struct ht_bucket);
    packet->header.opt = KV_OPT_INDEX;
    packet->header.total_size = sizeof(struct packet_header);

    int size = nn_send(fd_, packet, packet->header.total_size, 0);

    if (size != sizeof(struct packet_header))
    {
        LOG(ERROR) << "[DataAgent::Init] Init failed while send packet! " << nn_strerror(errno);
        return 0;
    }

    char *msg = NULL;
    char *p_ = index_->buf_;
    size_t count = 0;
    int left_ = bucket_size;

    LOG(INFO) << "[DataAgent::GetIndexData] Waiting receive index data!";

    while (true)
    {
        size = nn_recv(fd_, &msg, NN_MSG, 0);

        if (size > 0)
        {
            memcpy(p_, msg, size);
            count += size;
            p_ += size;
            left_ -= size;
            nn_freemsg(msg);

            if (left_ <= 0)
            {
                LOG(INFO) << "[DataAgent::GetIndexData] Receive finished! (" << count << ")";
                break;
            }
        }
    }

    index_->print();
    return 1;
}

int DataAgent::Append(KVString &key, KVString &val)
{
    write_opt_count_++;
#ifdef DEBUG_PRINT
    if (write_opt_count_ % 10000 == 0)
    {
        LOG(INFO) << "[DataAgent::Append] " << url_ << " is runnning (" << write_opt_count_ << ")";
    }
#endif

    packet->header.total_size = sizeof(struct packet_header) + KEY_LENGTH + VALUE_LENGTH;
    packet->header.opt = KV_OPT_PUT;
    memcpy(packet->buf_, key.Buf(), KEY_LENGTH);
    memcpy(packet->buf_ + KEY_LENGTH, val.Buf(), VALUE_LENGTH);

    uint64_t hash_val = offset_;
    bool res = index_->insert((uint8_t *)key.Buf(), hash_val);

    if (res != true)
    {
        LOG(ERROR) << "[DataAgent::Append] Index insert error!";
    }

    int size;
    size = nn_send(fd_, packet, packet->header.total_size, 0);

    if (size != packet->header.total_size)
    {
        LOG(INFO) << "[DataAgent::Append] Send failed while append and waiting to re-connect!";
        return 0;
    }

#ifdef NEED_ACK
    char *ack_ = NULL;
    size = nn_recv(fd_, &ack_, NN_MSG, 0);
    if (size != sizeof(struct packet_header))
    {
        return 0;
    }
    if (ack_ != NULL)
    {
        nn_freemsg(ack_);
    }
#endif

    offset_ += VALUE_LENGTH;
    return 1;
}

/*
int DataAgent::Get(KVString &key, KVString &val)
{
    char *v = new char[VALUE_LENGTH];
    uint64_t offset;
    bool res = index_->search((uint8_t *)key.Buf(), offset);

    if (!res)
    {
        return 0;
    }
    else
    {
        packet->header.opt = KV_OPT_GET;
        packet->header.offset = offset;
        packet->header.total_size = sizeof(struct packet_header);
        int size;
        size = nn_send(fd_, packet, packet->header.total_size, 0);
        size = nn_recv(fd_, v, VALUE_LENGTH, 0);
        val.Reset(v, VALUE_LENGTH);
        return 1;
    }
}
*/

int DataAgent::Get(KVString &key, KVString &val)
{
    read_opt_count_++;
#ifdef DEBUG_PRINT
    if (read_opt_count_ % 10000 == 0)
    {
        LOG(INFO) << "[DataAgent::Append] " << url_ << " is runnning (" << read_opt_count_ << ")";
    }
#endif

    char *v = new char[VALUE_LENGTH];
    int size;
    uint64_t offset;
    bool res = index_->search((uint8_t *)key.Buf(), offset);

    if (!res)
    {
        return 0;
    }
    else
    {
    re_check:
        read_cache_count_++;
        int rc = cache_->ReadKV(offset, v);

        if (rc != -1)
        {
            val.Reset(v, VALUE_LENGTH);
            return 1;
        }
        else
        {
            uint64_t cache_offset = CACHE_ALIGN(offset);
            uint8_t c_ = hot_[cache_offset];

            if (c_ < HOT_PARAM)
            {
                hot_[cache_offset]++;
                packet->header.opt = KV_OPT_GET;
                packet->header.offset = offset;
                packet->header.total_size = sizeof(struct packet_header);
                int size;
                size = nn_send(fd_, packet, packet->header.total_size, 0);
                size = nn_recv(fd_, v, VALUE_LENGTH, 0);
                val.Reset(v, VALUE_LENGTH);
                return 1;
            }
            else
            {
                packet->header.opt = KV_OPT_CACHE;
                packet->header.offset = offset;
                packet->header.total_size = sizeof(struct packet_header);
                size = nn_send(fd_, packet, packet->header.total_size, 0);

                cache_node *new_node = new cache_node();
                char *msg = NULL;
                char *p_ = (char *)new_node;
                int count = 0;
                int left_ = sizeof(struct cache_node);

#ifdef DEBUG_PRINT
                LOG(INFO) << "[DataAgent::Get] " << url_ << "waiting cache node (Read/Recv): " << read_cache_count_ << "/" << recv_cache_count_;
#endif
                while (true)
                {
                    size = nn_recv(fd_, &msg, NN_MSG, 0);
                    if (size > 0)
                    {
                        memcpy(p_, msg, size);
                        count += size;
                        p_ += size;
                        left_ -= size;
                        nn_freemsg(msg);
                        if (left_ <= 0)
                        {
                            break;
                        }
                    }
                }
#ifdef DEBUG_PRINT
                LOG(INFO) << "[DataAgent::Get] " << url_ << "got read/recv cahce node : " << read_cache_count_ << "/" << recv_cache_count_;
#endif
                cache_->Append(new_node);
                recv_cache_count_++;
                goto re_check;
            }
        }
    }
}