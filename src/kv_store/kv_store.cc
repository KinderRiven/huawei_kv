#include <iostream>
#include <string.h>

#include "tcp_server.h"
#include <pthread.h>
#include "utils.h"
#include "config.h"

INITIALIZE_EASYLOGGINGPP;

const char *exeName(const char *name)
{
    int pos = 0;
    if (name == nullptr || (pos = strlen(name)) < 1)
    {
        return nullptr;
    }

    for (; pos > 0; pos--)
    {
        if (name[pos - 1] == '/')
        {
            break;
        }
    }

    return name + pos;
}

void help(const char *name)
{
    std::cout << "usage: " << name << " host file_dir [clear]" << std::endl;
    std::cout << "   eg: " << name << " tcp://0.0.0.0 ./data" << std::endl;
    std::cout << "       " << name << " tcp://0.0.0.0 ./data clear" << std::endl;
    exit(-1);
}

void initLog(const char *name)
{
    el::Configurations conf;
    conf.setToDefault();
    char log_path[256] = "logs/";
    strcat(log_path, name);
    strcat(log_path, ".log");
    conf.set(el::Level::Global, el::ConfigurationType::Filename, log_path);
    el::Loggers::reconfigureAllLoggers(conf);
}

void kv_pause()
{
    system("stty raw -echo");
    std::cout << "press any key to exit ...";
    getchar();
    system("stty -raw echo");
    std::cout << std::endl;
}

struct thread_args
{
    int thread_id;
    char dir[64];
    char host[64];
};

static void *thread_task(void *a_)
{
    struct thread_args *args = (struct thread_args *)a_;
    TcpServer *server = new TcpServer();
    server->Init(args->thread_id, args->dir, args->host);
    server->Start();
    return NULL;
}

int main(int argc, char *argv[])
{
    START_EASYLOGGINGPP(argc, argv);
    const char *name = exeName(argv[0]);
    initLog(name);

    if (argc != 3 && argc != 4)
    {
        LOG(ERROR) << "param should be 3 or 4";
        help(name);
        return -1;
    }

    const char *host = argv[1];
    const char *dir = argv[2];
    bool clear = false;

    if (argc == 4)
    {
        if (strcmp("clear", argv[3]) != 0)
        {
            LOG(ERROR) << "param [4] should be \"clear\" if you want to clear local data";
            help(name);
            return -1;
        }
        else
        {
            clear = true;
        }
    }

    KV_LOG(INFO) << "[" << name << "] local store demo ...";
    KV_LOG(INFO) << "  >> bind  host : " << host;
    KV_LOG(INFO) << "  >> data  dir  : " << dir;
    KV_LOG(INFO) << "  >> clear dir  : " << clear;

    if (clear)
    {
        char rm_dir[128];
        for (int i = 0; i < NUM_STORE; i++)
        {
            int j = 0;
            while (true)
            {
                snprintf(rm_dir, sizeof(rm_dir), "%s_%d/data", dir, i);
                if (remove(rm_dir) != 0)
                {
                    break;
                }

                snprintf(rm_dir, sizeof(rm_dir), "%s_%d/index", dir, i);
                if (remove(rm_dir) != 0)
                {
                    break;
                }
                j++;
            }
            snprintf(rm_dir, sizeof(rm_dir), "%s_%d", dir, i);
            if (rmdir(rm_dir) == 0)
            {
                KV_LOG(INFO) << "  >> rmdir ok  : " << rm_dir;
            }
            else
            {
                KV_LOG(INFO) << "  >> rmdir failed  : " << rm_dir;
            }
        }
    }

    pthread_t thread_id[NUM_STORE];
    struct thread_args args[NUM_STORE];

    for (int i = 0; i < NUM_STORE; i++)
    {
        args[i].thread_id = i;
        strcpy(args[i].host, host);
        strcpy(args[i].dir, dir);
        pthread_create(thread_id + i, NULL, thread_task, (void *)&args[i]);
    }

    for (int i = 0; i < NUM_STORE; i++)
    {
        pthread_join(thread_id[i], NULL);
    }

    return 0;
}
