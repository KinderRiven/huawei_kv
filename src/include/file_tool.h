#ifndef INCLUDE_TOOL_H_
#define INCLUDE_TOOL_H_

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>

class FileTool
{
  public:
    static size_t file_write(const char *name, void *buff, size_t buff_size)
    {
        int fd = open(name, O_RDWR);
        size_t size = write(fd, buff, buff_size);
        close(fd);
        return size;
    }

    static size_t file_sync_write(const char *name, void *buff, size_t buff_size)
    {
        int fd = open(name, O_RDWR | O_CREAT, 0777);
        size_t size = write(fd, buff, buff_size);
        fsync(fd);
        close(fd);
        return size;
    }

    static size_t file_read(const char *name, void *buff, size_t size, size_t offset)
    {
        int fd = open(name, O_RDONLY);
        if (fd == -1)
        {
            printf("read error!\n");
        }
        int rd = pread(fd, buff, size, offset);
        if (rd == -1)
        {
            printf("read error!\n");
        }
        close(fd);
        return 0;
    }
};

#endif