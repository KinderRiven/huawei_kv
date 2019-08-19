#include "PFHT.h"
#include <assert.h>

#define NUMBER64_1 11400714785074694791ULL
#define NUMBER64_2 14029467366897019727ULL
#define NUMBER64_3 1609587929392839161ULL
#define NUMBER64_4 9650029242287828579ULL
#define NUMBER64_5 2870177450012600261ULL

#define hash_get64bits(x) hash_read64_align(x, align)
#define hash_get32bits(x) hash_read32_align(x, align)
#define shifting_hash(x, r) ((x << r) | (x >> (64 - r)))
#define TO64(x) (((U64_INT *)(x))->v)
#define TO32(x) (((U32_INT *)(x))->v)

typedef struct U64_INT
{
    uint64_t v;
} U64_INT;

typedef struct U32_INT
{
    uint32_t v;
} U32_INT;

static uint64_t hash_read64_align(const void *ptr, uint32_t align)
{
    if (align == 0)
    {
        return TO64(ptr);
    }
    return *(uint64_t *)ptr;
}

static uint32_t hash_read32_align(const void *ptr, uint32_t align)
{
    if (align == 0)
    {
        return TO32(ptr);
    }
    return *(uint32_t *)ptr;
}

/*
Function: string_key_hash_computation() 
        A hash function for string keys
*/
static uint64_t string_key_hash_computation(const void *data, uint64_t length, uint64_t seed, uint32_t align)
{
    const uint8_t *p = (const uint8_t *)data;
    const uint8_t *end = p + length;
    uint64_t hash;

    if (length >= 32)
    {
        const uint8_t *const limitation = end - 32;
        uint64_t v1 = seed + NUMBER64_1 + NUMBER64_2;
        uint64_t v2 = seed + NUMBER64_2;
        uint64_t v3 = seed + 0;
        uint64_t v4 = seed - NUMBER64_1;

        do
        {
            v1 += hash_get64bits(p) * NUMBER64_2;
            p += 8;
            v1 = shifting_hash(v1, 31);
            v1 *= NUMBER64_1;
            v2 += hash_get64bits(p) * NUMBER64_2;
            p += 8;
            v2 = shifting_hash(v2, 31);
            v2 *= NUMBER64_1;
            v3 += hash_get64bits(p) * NUMBER64_2;
            p += 8;
            v3 = shifting_hash(v3, 31);
            v3 *= NUMBER64_1;
            v4 += hash_get64bits(p) * NUMBER64_2;
            p += 8;
            v4 = shifting_hash(v4, 31);
            v4 *= NUMBER64_1;
        } while (p <= limitation);

        hash = shifting_hash(v1, 1) + shifting_hash(v2, 7) + shifting_hash(v3, 12) + shifting_hash(v4, 18);

        v1 *= NUMBER64_2;
        v1 = shifting_hash(v1, 31);
        v1 *= NUMBER64_1;
        hash ^= v1;
        hash = hash * NUMBER64_1 + NUMBER64_4;

        v2 *= NUMBER64_2;
        v2 = shifting_hash(v2, 31);
        v2 *= NUMBER64_1;
        hash ^= v2;
        hash = hash * NUMBER64_1 + NUMBER64_4;

        v3 *= NUMBER64_2;
        v3 = shifting_hash(v3, 31);
        v3 *= NUMBER64_1;
        hash ^= v3;
        hash = hash * NUMBER64_1 + NUMBER64_4;

        v4 *= NUMBER64_2;
        v4 = shifting_hash(v4, 31);
        v4 *= NUMBER64_1;
        hash ^= v4;
        hash = hash * NUMBER64_1 + NUMBER64_4;
    }
    else
    {
        hash = seed + NUMBER64_5;
    }

    hash += (uint64_t)length;

    while (p + 8 <= end)
    {
        uint64_t k1 = hash_get64bits(p);
        k1 *= NUMBER64_2;
        k1 = shifting_hash(k1, 31);
        k1 *= NUMBER64_1;
        hash ^= k1;
        hash = shifting_hash(hash, 27) * NUMBER64_1 + NUMBER64_4;
        p += 8;
    }

    if (p + 4 <= end)
    {
        hash ^= (uint64_t)(hash_get32bits(p)) * NUMBER64_1;
        hash = shifting_hash(hash, 23) * NUMBER64_2 + NUMBER64_3;
        p += 4;
    }

    while (p < end)
    {
        hash ^= (*p) * NUMBER64_5;
        hash = shifting_hash(hash, 11) * NUMBER64_1;
        p++;
    }

    hash ^= hash >> 33;
    hash *= NUMBER64_2;
    hash ^= hash >> 29;
    hash *= NUMBER64_3;
    hash ^= hash >> 32;

    return hash;
}

static uint64_t hash(const void *data, uint64_t length, uint64_t seed)
{
    if ((((uint64_t)data) & 7) == 0)
    {
        return string_key_hash_computation(data, length, seed, 1);
    }
    return string_key_hash_computation(data, length, seed, 0);
}

/*
Function: F_HASH()
        Compute the first hash value of a key-value item
*/
uint64_t F_HASH(PFHT *ht, const uint8_t *key)
{
    return (hash((void *)key, KEY_LENGTH, ht->f_seed));
}

/*
Function: S_HASH() 
        Compute the second hash value of a key-value item
*/
uint64_t S_HASH(PFHT *ht, const uint8_t *key)
{
    return (hash((void *)key, KEY_LENGTH, ht->s_seed));
}

/*
Function: F_IDX() 
        Compute the second hash location
*/
uint64_t F_IDX(uint64_t hashKey, uint64_t capacity)
{
    return hashKey % (capacity / 2);
}

/*
Function: S_IDX() 
        Compute the second hash location
*/
uint64_t S_IDX(uint64_t hashKey, uint64_t capacity)
{
    return hashKey % (capacity / 2) + capacity / 2;
}

/*
Function: generate_seeds() 
        Generate two randomized seeds for hash functions
*/
void generate_seeds(PFHT *ht)
{
    srand(time(NULL));
    do
    {
        // ht->f_seed = rand();
        // ht->s_seed = rand();
        // ht->f_seed = ht->f_seed << (rand() % 63);
        // ht->s_seed = ht->s_seed << (rand() % 63);
        ht->f_seed = 1234567;
        ht->s_seed = 8765432;
    } while (ht->f_seed == ht->s_seed);
}

PFHT::PFHT()
{
    generate_seeds(this);
    num_bucket = TOTAL_NUM_BUCKET;
    num_extra_bucket = TOTAL_NUM_EXTRA_BUCKET;
    buf_ = NULL;
}

PFHT::~PFHT()
{
    if (buf_ != NULL)
    {
        delete[] buf_;
    }
}

bool PFHT::init()
{
    size_t size_ = (num_bucket + num_extra_bucket) * sizeof(struct ht_bucket);
    buf_ = new char[size_];
    partition = (struct ht_bucket *)buf_;
    extra_partition = (struct ht_bucket *)(buf_ + num_bucket * sizeof(struct ht_bucket));
    return true;
}

bool PFHT::insert(uint8_t *key_, uint64_t offset)
{
    bool flag = false;
    bool same = false;
    uint64_t f_hash = F_HASH(this, key_);
    uint64_t s_hash = S_HASH(this, key_);
    uint64_t bucket_id1 = f_hash % num_bucket;
    uint64_t bucket_id2 = s_hash % num_bucket;
    uint64_t key = *((uint64_t *)key_);

    // uint64_t hash64_1 = CityHash64WithSeed((const char *)key, KEY_LENGTH, SEED1);
    // uint64_t hash64_2 = CityHash64WithSeed((const char *)key, KEY_LENGTH, SEED2);
    // uint64_t bucket_id1 = hash64_1 % num_bucket;
    // uint64_t bucket_id2 = hash64_2 % num_bucket;

    struct ht_bucket *bucket1 = (struct ht_bucket *)partition + bucket_id1;
    struct ht_bucket *bucket2 = (struct ht_bucket *)partition + bucket_id2;

    if (bucket_id1 == bucket_id2)
    {
        same = true;
    }

    for (int i = 0; i < NUM_SLOT; i++)
    {
        if (bucket1->bitmap & (1 << i))
        {
            if (bucket1->key[i] == key)
            {
                bucket1->addr[i] = offset;
                flag = true;
                break;
            }
        }
        else
        {
            bucket1->addr[i] = offset;
            bucket1->key[i] = key;
            flag = true;
            bucket1->bitmap |= (1 << i);
            break;
        }

        if (!same)
        {
            if (bucket2->bitmap & (1 << i))
            {
                if (bucket2->key[i] == key)
                {
                    bucket2->addr[i] = offset;
                    flag = true;
                }
            }
            else
            {
                bucket2->addr[i] = offset;
                bucket2->key[i] = key;
                flag = true;
                bucket2->bitmap |= (1 << i);
                break;
            }
        }
    }

    if (flag == false)
    {
        for (int i = 0; i < this->num_extra_bucket; i++)
        {
            struct ht_bucket *extra_bucket = (struct ht_bucket *)extra_partition + i;
            for (int j = 0; j < NUM_SLOT; j++)
            {
                if (extra_bucket->bitmap & (1 << i))
                {
                    if (extra_bucket->key[i] == key)
                    {
                        extra_bucket->addr[i] = offset;
                        flag = true;
                        break;
                    }
                }
                else
                {
                    extra_bucket->addr[i] = offset;
                    extra_bucket->key[i] = key;
                    flag = true;
                    extra_bucket->bitmap |= (1 << i);
                    break;
                }
            }
            if (flag)
            {
                break;
            }
        }
    }

    return flag;
}

bool PFHT::search(uint8_t *key_, uint64_t &offset)
{
    uint64_t f_hash = F_HASH(this, key_);
    uint64_t s_hash = S_HASH(this, key_);
    uint64_t bucket_id1 = f_hash % num_bucket;
    uint64_t bucket_id2 = s_hash % num_bucket;
    uint64_t key = *((uint64_t *)key_);

    // uint64_t hash64_1 = CityHash64WithSeed((const char *)key, KEY_LENGTH, SEED1);
    // uint64_t hash64_2 = CityHash64WithSeed((const char *)key, KEY_LENGTH, SEED2);
    // uint64_t bucket_id1 = hash64_1 % num_bucket;
    // uint64_t bucket_id2 = hash64_2 % num_bucket;

    struct ht_bucket *bucket1 = (struct ht_bucket *)partition + bucket_id1;
    struct ht_bucket *bucket2 = (struct ht_bucket *)partition + bucket_id2;

    for (int i = 0; i < NUM_SLOT; i++)
    {
        if (bucket1->bitmap & (1 << i))
        {
            if (bucket1->key[i] == key)
            {
                offset = bucket1->addr[i];
                return true;
            }
        }

        if (bucket2->bitmap & (1 << i))
        {
            if (bucket2->key[i] == key)
            {
                offset = bucket2->addr[i];
                return true;
            }
        }
    }

    for (int i = 0; i < this->num_extra_bucket; i++)
    {
        struct ht_bucket *extra_bucket = (struct ht_bucket *)extra_partition + i;

        for (int j = 0; j < NUM_SLOT; j++)
        {
            if (extra_bucket->bitmap & (1 << i))
            {
                if (extra_bucket->key[i] == key)
                {
                    offset = extra_bucket->addr[i];
                    return true;
                }
            }
        }
    }
    return false;
}

void PFHT::print()
{
    size_t total_size = (num_bucket + num_extra_bucket) * sizeof(struct ht_bucket);
    uint64_t slot_used = 0;
    uint64_t total_slot = 0;

    for (int j = 0; j < num_bucket; j++)
    {
        struct ht_bucket *bucket = (struct ht_bucket *)partition + j;
        for (int k = 0; k < NUM_SLOT; k++)
        {
            if (bucket->bitmap & (1 << k))
            {
                slot_used++;
            }
            total_slot++;
        }
    }

    for (int i = 0; i < num_extra_bucket; i++)
    {
        struct ht_bucket *bucket = (struct ht_bucket *)extra_partition + i;
        for (int j = 0; j < NUM_SLOT; j++)
        {
            if (bucket->bitmap & (1 << j))
            {
                slot_used++;
            }
            total_slot++;
        }
    }

    LOG(INFO) << "[HashTable]";
    LOG(INFO) << "[Bucket/Slot]:" << num_bucket << "/" << num_extra_bucket << "/" << NUM_SLOT;
    LOG(INFO) << "[Memory Usage]:" << total_size / (1024 * 1024);
    LOG(INFO) << "[Slot Used/Total]:" << slot_used << "/" << total_slot << ")";
}