#include "easylogging++.h"

#ifndef INCLUDE_CONFIG_H_

#define SERVER_START_PORT (9527)

#define KEY_LENGTH (8)
#define VALUE_LENGTH (4096)

#define NUM_AGENT (16)
#define NUM_STORE (16)

#define TOTAL_NUM_BUCKET ((12000000) / (NUM_STORE))
#define TOTAL_NUM_EXTRA_BUCKET ((1000000) / (NUM_STORE))

// #define DEBUG_PRINT
#define NEED_ACK (5)

#endif