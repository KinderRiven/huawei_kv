#include "tcp_server.h"
#include "utils.h"
#include "nanomsg/nn.h"
#include "nanomsg/pair.h"

#include <unistd.h>
#include <thread>
#include <memory>
#include <chrono>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "cache.h"

#define NN_LOG(level, msg) KV_LOG(level) << msg << " failed. error: " << nn_strerror(nn_errno())

TcpServer::TcpServer()
{
    LOG(INFO) << "[TcpServer::Init] new server prepare to create!";
    fd_ = -1;
    store_ = NULL;

    rep_packet = Packet::GetNewPacket(0);
    buf_ = new char[VALUE_LENGTH * 2];
    value_buf_ = (char *)(((uint64_t)buf_ + 4096) & (~(4096 - 1)));
    assert((uint64_t)value_buf_ % 4096 == 0);
    key_buf_ = new char[KEY_LENGTH];

    write_opt_count_ = 0;
    read_opt_count_ = 0;
    cache_opt_count_ = 0;
}

TcpServer::~TcpServer()
{
    Stop();
}

int TcpServer::Init(int id, const char dir[], const char url[])
{
    store_ = new KVStore();
    store_->Init(dir, id);

    char url_[64];
    snprintf(url_, sizeof(url_), "%s:%d", url, SERVER_START_PORT + id);

    while (true)
    {
        fd_ = Connect(url_);

        if (fd_ != -1)
        {
            break;
        }
        else
        {
            LOG(ERROR) << "[TcpServer::Init] someting wrong while creating new server! [ID:" << id << "][FD:" << fd_ << "[URL:" << url_ << "]";
            sleep(5);
        }
    }

    LOG(INFO) << "[TcpServer::Init] init new server! [ID:" << id << "][FD:" << fd_ << "[URL:" << url_ << "]";
    return 1;
}

// Use pair mode to send/recv to reduce block
int TcpServer::Connect(const char url[])
{
    int fd = nn_socket(AF_SP, NN_PAIR); // use pari method to establish socket

    if (fd < 0)
    {
        return -1;
    }

    int time1 = 5000;
    int time2 = 5000;
    assert(nn_setsockopt(fd, 0, NN_SNDTIMEO, &time1, sizeof(time1) >= 0));
    assert(nn_setsockopt(fd, 0, NN_RCVTIMEO, &time2, sizeof(time2) >= 0));

    if (nn_bind(fd, url) < 0)
    {
        nn_close(fd);
        return -1;
    }
    return fd;
}

int TcpServer::GetKV(Packet *packet)
{
#ifdef DEBUG_PRINT
    read_opt_count_++;
    if (read_opt_count_ % 5000 == 0)
    {
        LOG(INFO) << "[TcpServer::GetKV] " << fd_ << " is runnning (" << read_opt_count_ << ")";
    }
#endif

    int ret = store_->Get(packet->header.offset, value_buf_);
    int size = nn_send(fd_, value_buf_, VALUE_LENGTH, NN_DONTWAIT); // without block

    if (size != VALUE_LENGTH)
    {
        LOG(ERROR) << "[TcpServer::Start] Someting error while handling KV_OPT_GET (1). " << nn_strerror(errno);
        return 0;
    }

    uint64_t offset = CACHE_ALIGN(packet->header.offset);
    hot_[offset]++;

    if (hot_[offset] >= HOT_PARAM / 2)
    {
        store_->AppendNewBlock(offset);
    }

    return 1;
}

// prefetch read data block (8MB)
/*
int TcpServer::GetKV(Packet *packet)
{
    uint64_t offset = packet->header.offset;
    int ret = store_->Get(packet->header.offset, value_buf_);
    int size = nn_send(fd_, value_buf_, VALUE_LENGTH, 0); // without block

    if (ret == NUM_CACHE_SLOT - 1)
    {
        store_->PrefGetCacheBlock();
    }
    else if (ret == -1)
    {
        store_->AppendNewBlock(offset);
    }
    return 1;
}
*/

int TcpServer::SetKV(Packet *packet)
{
    write_opt_count_++;
#ifdef DEBUG_PRINT
    if (write_opt_count_ % 10000 == 0)
    {
        LOG(INFO) << "[TcpServer::SetKV] " << fd_ << " is runnning (" << write_opt_count_ << ")";
    }
#endif

    uint64_t buffer_size = packet->header.total_size - sizeof(struct packet_header);

    if (buffer_size != (KEY_LENGTH + VALUE_LENGTH))
    {
        LOG(ERROR) << "[TcpServer::Start] Someting error while handling KV_OPT_PUT (1). " << nn_strerror(errno);
        return 0;
    }

    memcpy(key_buf_, packet->buf_, KEY_LENGTH);
    memcpy(value_buf_, packet->buf_ + KEY_LENGTH, VALUE_LENGTH);
    store_->Set(key_buf_, value_buf_);

#ifdef NEED_ACK
    // pair mode don't consider to response
    nn_send(fd_, packet, sizeof(struct packet_header), 0);
#endif

    return 1;
}

int TcpServer::GetCacheBlock(Packet *packet)
{
#ifdef DEBUG_PRINT
    cache_opt_count_++;
    if (cache_opt_count_ % 1000 == 0)
    {
        LOG(INFO) << "[TcpServer::GetCacheBlock] " << fd_ << " is runnning (" << cache_opt_count_ << ")";
    }
#endif

    cache_node *new_node = new cache_node();
    int ret = store_->GetCacheBlock(packet->header.offset, (char *)new_node);
    char *p_ = (char *)new_node;
    int left_ = sizeof(struct cache_node);
    int count = 0;
    int size;

    while (true)
    {
        if (left_ < 4096)
        {
            size = nn_send(fd_, p_, left_, 0);
            count += left_;
        }
        else
        {
            size = nn_send(fd_, p_, 4096, 0);
            count += 4096;
        }

        left_ -= 4096;
        p_ += 4096;

        if (left_ <= 0)
        {
            break;
        }
    }

    delete new_node;
    return 1;
}

int TcpServer::GetMetaData()
{
    rep_packet->header.offset = store_->value_store->offset;
    int size = nn_send(fd_, rep_packet, sizeof(struct packet_header), 0);

    if (size != sizeof(struct packet_header))
    {
        LOG(ERROR) << "[TcpServer::Run] Something bad while handle KV_OPT_METEDATA (1) ..." << nn_strerror(errno);
        return 0;
    }

    return 1;
}

int TcpServer::GetIndexData()
{
    size_t alloc_size_ = (TOTAL_NUM_BUCKET + TOTAL_NUM_EXTRA_BUCKET) * sizeof(struct ht_bucket);
    char *alloc_ = new char[alloc_size_];
    char *p_ = alloc_;
    memset(alloc_, 0, alloc_size_);

    // TODO index recovery
    char *partition = alloc_;
    char *extra_partition = alloc_ + (TOTAL_NUM_BUCKET * sizeof(struct ht_bucket));
    store_->RecoveryIndex(partition, extra_partition);

    int left_ = alloc_size_;
    int size;
    int count = 0;

    // send 4KB packet each
    while (true)
    {
        if (left_ < 4096)
        {
            size = nn_send(fd_, p_, left_, 0);
            count += left_;
        }
        else
        {
            size = nn_send(fd_, p_, 4096, 0);
            count += 4096;
        }

        left_ -= 4096;
        p_ += 4096;

        if (left_ <= 0)
        {
            LOG(INFO) << "[TcpServer::GetIndexData] Send finished! (" << count << ")";
            break;
        }
    }

    delete[] alloc_;
    return 1;
}

void TcpServer::Start()
{
    LOG(INFO) << "[TcpServer::Start] Everything is ok and waiting msg! [FD:" << fd_ << "]";

    while (true)
    {
        char *msg = NULL;
        int size = nn_recv(fd_, &msg, NN_MSG, 0); // Receive a packet header

        if (size >= sizeof(struct packet_header))
        {
            uint64_t opt = ((Packet *)msg)->header.opt;
            if (size != ((Packet *)msg)->header.total_size)
            {
                LOG(ERROR) << "[TcpServer::Start] Unexpected packet size! (" << size << ")(" << opt << ")";
                continue;
            }
            switch (opt)
            {
            case KV_OPT_PUT:
                SetKV((Packet *)msg);
                break;
            case KV_OPT_GET:
                GetKV((Packet *)msg);
                break;
            case KV_OPT_INDEX:
                GetIndexData();
                break;
            case KV_OPT_METEDATA:
                LOG(INFO) << "[TcpServer::Start] Client want to get metadata!";
                GetMetaData();
                break;
            case KV_OPT_CLOSE:
                Print();
                break;
            case KV_OPT_CACHE:
                GetCacheBlock((Packet *)msg);
                break;
            default:
                LOG(ERROR) << "[TcpServer::Start] Undefine packet type (1). ";
                break;
            }
        }
        else
        {
            LOG(ERROR) << "[TcpServer::Start] Bad packet size (less than packet header)...";
        }
        if (msg != NULL)
        {
            nn_freemsg(msg);
        }
    }

    return;
}

void TcpServer::Stop()
{
    if (fd_ != -1)
    {
        nn_close(fd_);
        fd_ = -1;
    }
    if (store_ != NULL)
    {
        store_->Close();
        store_ = NULL;
    }
}

int TcpServer::Print()
{
    store_->value_cache->Print();
    int size = nn_send(fd_, rep_packet, sizeof(struct packet_header), 0);
    return 1;
}