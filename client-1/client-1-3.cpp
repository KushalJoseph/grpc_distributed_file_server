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

int pfs_print_stat(int client_id) {
    struct pfs_execstat mystat = {0};
    int ret = pfs_execstat(&mystat);
    if (ret != -1) {
        printf("%s: client_id: %d\n", __func__, client_id);
        printf("%s: num_read_hits: %ld, num_write_hits: %ld\n", __func__, mystat.num_read_hits, mystat.num_write_hits);
        printf("%s: num_evictions: %ld, num_writebacks: %ld, num_invalidations: %ld\n", __func__, mystat.num_evictions, mystat.num_writebacks, mystat.num_invalidations);
        printf("%s: num_close_writebacks: %ld, num_close_evictions: %ld\n", __func__, mystat.num_close_writebacks, mystat.num_close_evictions);
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
    ssize_t nread = pread(input_fd, (void *)buf, 2048, 2048);
    if (nread != 2048) {
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

    // Write the byte 2048~4095 to pfs_file1 at offset 2048
    int ret = pfs_write(pfs_fd, (void *)buf, 2048, 2048);
    if (ret == -1) {
        fprintf(stderr, "Write error to PFS file.\n");
        return -1;
    } else
        printf("%s:%s: Wrote %d bytes to the PFS file.\n", __FILE__, __func__, ret);

    // Read the entire pfs_file1
    size_t pfs_buf_size = 16 * 1024;
    char *pfs_buf = (char *)malloc(pfs_buf_size);
    memset(pfs_buf, 0, pfs_buf_size);
    ret = pfs_read(pfs_fd, (void *)pfs_buf, 4096, 0);

    // Close and delete the pfs_file1
    if (pfs_close(pfs_fd) == -1) {
        fprintf(stderr, "Error closing PFS file.\n");
        return -1;
    }
    if (pfs_delete("pfs_file1") == -1) {
        fprintf(stderr, "PFS file delete error.\n");
        return -1;
    }
    if (pfs_open("pfs_file1", 1) != -1) {
        fprintf(stderr, "File deleted, but file opened.\n");
        // return -1; // THIS IS AN ERROR, but continue for explanation purposes
    }

    // Write the PFS file locally
    std::string output_filename("output.txt");
    int output_fd = open(output_filename.c_str(), O_WRONLY | O_CREAT, 0644);
    ssize_t nwrite = pwrite(output_fd, pfs_buf, 4096, 0);
    if (nwrite != 4096) {
        fprintf(stderr, "pwrite() failed\n");
        return -1;
    }
    close(output_fd);

    // Compare 4096 bytes of myfile.txt and output.txt
    input_fd = open(input_filename.c_str(), O_RDONLY);
    nread = pread(input_fd, buf, 4096, 0);
    if (nread != 4096) {
        fprintf(stderr, "pread() error.\n");
        return -1;
    }
    int cmp = memcmp(buf, pfs_buf, 4096);
    if (cmp != 0) {
        printf("myfile.txt and PFS file memcmp() failed.\n");
        // return -1; // THIS IS AN ERROR, but continue for explanation purposes
    }
    close(input_fd);

    if (pfs_print_stat(client_id) == -1) {
        fprintf(stderr, "stat print failed.\n");
        return -1;
    }

    ret = pfs_finish(client_id);
    if (ret == -1) {
        fprintf(stderr, "pfs_finish() failed.\n");
        return -1;
    }

    free(buf);
    free(pfs_buf);
    printf("%s:%s: Finish!\n", __FILE__, __func__);
    return 0;
}
