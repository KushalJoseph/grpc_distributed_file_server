#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "pfs_common/pfs_common.hpp"
#include "pfs_client/pfs_api.hpp"
#include "pfs_client/pfs_cache.hpp"

int pfs_print_meta(int pfs_fd, int client_id) {
    struct pfs_metadata mymeta = {0};
    int ret = pfs_fstat(pfs_fd, &mymeta);
    if (ret != -1) {
        printf("%s: PFS fd: %d, Client id: %d\n", __func__, pfs_fd, client_id);
        printf("%s: File name: %s, size: %lu\n", __func__, mymeta.filename, mymeta.file_size);
        printf("%s: Time of creation: %s", __func__, ctime(&(mymeta.ctime)));
        printf("%s: Last modification: %s", __func__, ctime(&(mymeta.mtime)));
    }
    return ret;
}

int main(int argc, char *argv[]) {
    printf("%s:%s: Start! Hostname: %s, IP: %s\n", __FILE__, __func__, getMyHostname().c_str(), getMyIP().c_str());
    if (argc < 2) {
        fprintf(stderr, "%s: usage: ./sample1-1 <input filename>\n", __func__);
        return -1;
    }

    // Open a file
    std::string input_filename(argv[1]);
    int input_fd = open(input_filename.c_str(), O_RDONLY);
    char *buf = (char *)malloc(8 * 1024);
    ssize_t nread = pread(input_fd, (void *)buf, 1024, 1024);
    if (nread != 1024) {
        fprintf(stderr, "pread() error.\n");
        return -1;
    }
    close(input_fd);

    // Initialize the PFS client
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "pfs_initialize() failed.\n");
        return -1;
    }

    // Open the PFS file in write mode
    int pfs_fd = pfs_open("pfs_file1", 2);
    if (pfs_fd == -1) {
        fprintf(stderr, "Error opening PFS file.\n");
        return -1;
    }

    // Write the byte 1024~2047 to pfs_file1 at offset 1024
    int ret = pfs_write(pfs_fd, (void *)buf, 1024, 1024);
    if (ret == -1) {
        fprintf(stderr, "Write error to PFS file.\n");
        return -1;
    } else
        printf("%s:%s: Wrote %d bytes to the PFS file.\n", __FILE__, __func__, ret);

    // Print metadata
    if (pfs_print_meta(pfs_fd, client_id) == -1) {
        fprintf(stderr, "PFS metadata print error.\n");
        return -1;
    };

    ret = pfs_close(pfs_fd);
    if (ret == -1) {
        fprintf(stderr, "Error closing PFS file.\n");
        return -1;
    }

    ret = pfs_finish(client_id);
    if (ret == -1) {
        fprintf(stderr, "pfs_finish() failed.\n");
        return -1;
    }

    free(buf);
    printf("%s:%s: Finish!\n", __FILE__, __func__);
    return 0;
}
