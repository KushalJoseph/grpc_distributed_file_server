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
        // return -1;
    }

    // Open the PFS file in write mode
    int pfs_fd = pfs_open("pfs_file1", 2);
    if (pfs_fd == -1) {
        fprintf(stderr, "Error opening PFS file.\n");
        return -1;
    }

    // Write the byte 0~1023 to pfs_file1 at offset 0
    ret = pfs_write(pfs_fd, (void *)buf, 8192, 0);
    if (ret == -1) {
        fprintf(stderr, "Write error to PFS file.\n");
        return -1;
    } else
        printf("%s:%s: Wrote %d bytes to the PFS file.\n", __FILE__, __func__, ret);

    std::this_thread::sleep_for(std::chrono::seconds(3));
    std::cout << std::endl << std::endl << std::endl << std::endl << std::endl;

    char *read_content = (char*) malloc(50);
    ret = pfs_read(pfs_fd, (void *)read_content, 50, 1024);
    if (ret == -1) {
        fprintf(stderr, "Read error to PFS file.\n");
        return -1;
    } else {
        printf("%s:%s: Read the following %d bytes from the PFS file:\n", __FILE__, __func__, ret);
        std::cout << "\033[34m" << (std::string(read_content)) << "\033[0m" << std::endl;
    }
    std::cout << std::endl << std::endl << std::endl << std::endl;

    read_content = (char*) malloc(50);
    ret = pfs_read(pfs_fd, (void *)read_content, 50, 2048);
    if (ret == -1) {
        fprintf(stderr, "Read error to PFS file.\n");
        return -1;
    } else {
        printf("%s:%s: Read the following %d bytes from the PFS file.\n", __FILE__, __func__, ret);
        std::cout << "\033[34m" << (std::string(read_content)) << "\033[0m" << std::endl;
    }
    std::cout << std::endl << std::endl << std::endl << std::endl << std::endl;

    read_content = (char*) malloc(50);
    ret = pfs_read(pfs_fd, (void *)read_content, 50, 2048);
    if (ret == -1) {
        fprintf(stderr, "Read error to PFS file.\n");
        return -1;
    } else {
        printf("%s:%s: Read the following %d bytes from the PFS file.\n", __FILE__, __func__, ret);
        std::cout << "\033[34m" << (std::string(read_content)) << "\033[0m" << std::endl;
    }
    read_content = (char*) malloc(50);
    ret = pfs_read(pfs_fd, (void *)read_content, 50, 1048);
    if (ret == -1) {
        fprintf(stderr, "Read error to PFS file.\n");
        return -1;
    } else {
        printf("%s:%s: Read the following %d bytes from the PFS file.\n", __FILE__, __func__, ret);
        std::cout << "\033[34m" <<(std::string(read_content)) << "\033[0m" << std::endl;
    }
    read_content = (char*) malloc(50);
    ret = pfs_read(pfs_fd, (void *)read_content, 50, 2048);
    if (ret == -1) {
        fprintf(stderr, "Read error to PFS file.\n");
        return -1;
    } else {
        printf("%s:%s: Read the following %d bytes from the PFS file.\n", __FILE__, __func__, ret);
        std::cout << "\033[34m" <<(std::string(read_content)) << "\033[0m" << std::endl;
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

    struct pfs_metadata mymeta = {0};
    ret = pfs_fstat(pfs_fd, &mymeta);
    if (ret != -1) {
        std::cout << mymeta.to_string() << std::endl;
    } else {
        fprintf(stderr, "File Metadata Read error to PFS file.\n");
        return -1;
    }

    free(buf);
    printf("%s:%s: Finish!\n", __FILE__, __func__);
    return 0;
}
