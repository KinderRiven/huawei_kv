/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *  * * * * * * * * * * *
 *            Copyight (C) 2018 Institute of Computing Technology, CAS
 *               Author : Shukai Han (Email : hanshukai@ict.ac.cn)
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *  * * * * * * * * * * *
 *                                PFHT
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *  * * * * * * * * * * *
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE. 
 * 
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *  * * * * * * * * * * */

#ifndef NVM_HASH_TABLE_H_
#define NVM_HASH_TABLE_H_

#include "city.h"
#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/time.h>

#include <string>
#include <map>

#define NUM_SLOT (8)
#define SEED1 (4396)
#define SEED2 (1995)

struct ht_bucket
{
  uint8_t bitmap;
  uint64_t key[NUM_SLOT];
  uint64_t addr[NUM_SLOT];
};

class PFHT
{
public:
  PFHT();
  ~PFHT();

public:
  bool init();
  bool insert(uint8_t *key, uint64_t offset);
  bool search(uint8_t *key, uint64_t &offset);
  void print();

public:
  size_t num_bucket;
  size_t num_extra_bucket;
  struct ht_bucket *partition;
  struct ht_bucket *extra_partition;
  char *buf_;

public:
  uint64_t f_seed;
  uint64_t s_seed;
};

#endif