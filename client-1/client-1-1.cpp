#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "pfs_common/pfs_common.hpp"
#include "pfs_client/pfs_api.hpp"

int main(int argc, char *argv[]) {
    printf("%s:%s: Start! Hostname: %s, IP: %s\n", __FILE__, __func__, getMyHostname().c_str(), getMyIP().c_str());
    if (argc < 2) {
        fprintf(stderr, "%s: usage: ./sample1-1 <input filename>\n", __func__);
        return -1;
    }

    // Open a file
    std::string input_filename(argv[1]);
    int input_fd = open(input_filename.c_str(), O_RDONLY);
    char *buf = (char *)malloc(20 * 1024);
    ssize_t nread = pread(input_fd, (void *)buf, 1024 * 20, 0);
    if (nread != 1024 * 20) {
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
    std::cout << "My client ID: " << client_id << std::endl;

    // Create a PFS file
    int ret;
    ret = pfs_create("pfs_file1", 3); // stripe_width
    if (ret == -1) {
        fprintf(stderr, "Unable to create a PFS file.\n");
        return -1;
    }
    // ret = pfs_create("pfs_file2", 3);
    // if (ret == -1) {
    //     fprintf(stderr, "Unable to create a PFS file.\n");
    //     return -1;
    // }

    // Open the PFS file in write mode
    int pfs_fd = pfs_open("pfs_file1", 2);
    if (pfs_fd == -1) {
        fprintf(stderr, "Error opening PFS file.\n");
        return -1;
    }
    // int fd2 = pfs_open("pfs_file2", 2);
    // if (fd2 == -1) {
    //     fprintf(stderr, "Error opening PFS file.\n");
    //     return -1;
    // }

    // Write the byte 0~1023 to pfs_file1 at offset 0
    ret = pfs_write(pfs_fd, (void *)buf, 2000, 0);
    if (ret == -1) {
        fprintf(stderr, "Write error to PFS file.\n");
        return -1;
    } else
        printf("%s:%s: Wrote %d bytes to the PFS file.\n", __FILE__, __func__, ret);

    std::this_thread::sleep_for(std::chrono::seconds(5));

    char *read_content = (char*) malloc(50);
    ret = pfs_read(pfs_fd, (void *)read_content, 50, 0);
    if (ret == -1) {
        fprintf(stderr, "Read error to PFS file.\n");
        return -1;
    } else {
        printf("%s:%s: Read %d bytes from the PFS file.\n", __FILE__, __func__, ret);
        std::cout << (std::string(read_content)) << std::endl;
    }
    // every request's buffer is fresh
    // ret = pfs_write(pfs_fd, (void *)buf, 1000, 1024);
    // if (ret == -1) {
    //     fprintf(stderr, "Write error to PFS file.\n");
    //     return -1;
    // } else
    //     printf("%s:%s: Wrote %d bytes to the PFS file.\n", __FILE__, __func__, ret);

    // std::cout << read_content << std::endl << std::endl;

    // struct pfs_metadata mymeta = {0};
    // ret = pfs_fstat(pfs_fd, &mymeta);
    // if (ret != -1) {
    //     std::cout << mymeta.to_string() << std::endl;
    // } else {
    //     fprintf(stderr, "File Metadata Read error to PFS file.\n");
    //     return -1;
    // }

    // mymeta = {0};
    // ret = pfs_fstat(fd2, &mymeta);
    // if (ret != -1) {
    //     std::cout << mymeta.to_string() << std::endl;
    // } else {
    //     fprintf(stderr, "File Metadata Read error to PFS file.\n");
    //     return -1;
    // }

    // char *r2 = (char*) malloc(40);
    // ret = pfs_read(pfs_fd, (void *)r2, 40, 1000);
    // if (ret == -1) {
    //     fprintf(stderr, "Read error to PFS file.\n");
    //     return -1;
    // } else
    //     printf("%s:%s: Read %d bytes from the PFS file.\n", __FILE__, __func__, ret);

    // std::cout << "FINAL Read1: " << std::endl;
    // std::cout << r2 << std::endl << std::endl;

    // ret = pfs_read(pfs_fd, (void *)buf, 2000, 2000);
    // if (ret == -1) {
    //     fprintf(stderr, "Read error to PFS file.\n");
    //     return -1;
    // } else
    //     printf("%s:%s: Read %d bytes from the PFS file.\n", __FILE__, __func__, ret);

    // std::cout << "Read2: " << std::endl;
    // std::cout << buf << std::endl << std::endl;

    // ret = pfs_read(pfs_fd, (void *)buf, 10000, 1000);
    // if (ret == -1) {
    //     fprintf(stderr, "Read error to PFS file.\n");
    //     return -1;
    // } else
    //     printf("%s:%s: Read %d bytes from the PFS file.\n", __FILE__, __func__, ret);

    // ret = pfs_read(pfs_fd, (void *)buf, 1069, 69);
    // if (ret == -1) {
    //     fprintf(stderr, "Read error to PFS file.\n");
    //     return -1;
    // } else
    //     printf("%s:%s: Read %d bytes from the PFS file.\n", __FILE__, __func__, ret);

    // ret = pfs_write(pfs_fd, (void *)buf, 500, 1000);
    // if (ret == -1) {
    //     fprintf(stderr, "Write error to PFS file.\n");
    //     return -1;
    // } else
    //     printf("%s:%s: Wrote %d bytes to the PFS file.\n", __FILE__, __func__, ret);

    // ret = pfs_write(pfs_fd, (void *)buf, 500, 200);
    // if (ret == -1) {
    //     fprintf(stderr, "Write error to PFS file.\n");
    //     return -1;
    // } else
    //     printf("%s:%s: Wrote %d bytes to the PFS file.\n", __FILE__, __func__, ret);

    ret = pfs_close(pfs_fd);
    if (ret == -1) {
        fprintf(stderr, "Error closing PFS file.\n");
        return -1;
    }
    // ret = pfs_close(fd2);
    // if (ret == -1) {
    //     fprintf(stderr, "Error closing PFS file.\n");
    //     return -1;
    // }

    // ret = pfs_delete("pfs_file2");
    // if (ret == -1) {
    //     fprintf(stderr, "Delete error PFS file.\n");
    //     return -1;
    // } else
    //     printf("Deleted");

    // ret = pfs_delete("pfs_file1");
    // if (ret == -1) {
    //     fprintf(stderr, "Delete error PFS file.\n");
    //     return -1;
    // } else
    //     printf("Deleted");

    // ret = pfs_read(pfs_fd, (void *)read_content, 40, 0);
    // if (ret == -1) {
    //     fprintf(stderr, "Read error to PFS file.\n");
    //     return -1;
    // } else
    //     printf("%s:%s: Read %d bytes from the PFS file.\n", __FILE__, __func__, ret);

    // ret = pfs_finish(client_id);
    // if (ret == -1) {
    //     fprintf(stderr, "pfs_finish() failed.\n");
    //     return -1;
    // }

    // free(buf);
    // printf("%s:%s: Finish!\n", __FILE__, __func__);
    return 0;
}
