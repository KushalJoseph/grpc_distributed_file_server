#pragma once

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstdbool>
#include <vector>
#include <list>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <semaphore>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <sstream>

#include "pfs_common/pfs_config.hpp"
#include "pfs_common/pfs_common.hpp"

struct pfs_filerecipe {
    int stripe_width;
    std::vector<std::vector<int>> distribution; // {fileserver#: startByte (incl), endByte (incl)}

    std::string to_string() const {
        std::ostringstream oss;
        oss << "Stripe Width: " << stripe_width << "\n";
        oss << "Distribution:\n";
        for (size_t i=0; i<distribution.size(); i++) {
            oss << "  FileServer " << i 
                    << ": [" << distribution[i][0] << ", " << distribution[i][1] << "]\n";
        }
        return oss.str();
    }
};

struct pfs_metadata {
    // Given metadata
    char filename[256];
    uint64_t file_size;
    time_t ctime;
    time_t mtime;
    struct pfs_filerecipe recipe;

    // Additional...
    std::string to_string() const {
        std::ostringstream oss;
        oss << "Filename: " << filename << "\n";
        oss << "File Size: " << file_size << " bytes\n";
        
        // char ctime_str[20], mtime_str[20];
        // struct tm* ctm = localtime(&ctime);
        // struct tm* mtm = localtime(&mtime);
        // strftime(ctime_str, sizeof(ctime_str), "%Y-%m-%d %H:%M:%S", ctm);
        // strftime(mtime_str, sizeof(mtime_str), "%Y-%m-%d %H:%M:%S", mtm);
        // oss << "Creation Time: " << ctime_str << "\n";
        // oss << "Modification Time: " << mtime_str << "\n";

        oss << "File Recipe:\n" << recipe.to_string();
        return oss.str();
    }
};

struct pfs_execstat {
    long num_read_hits;
    long num_write_hits;
    long num_evictions;
    long num_writebacks;
    long num_invalidations;
    long num_close_writebacks;
    long num_close_evictions;
};

int pfs_initialize();
int pfs_finish(int client_id);
int pfs_create(const char *filename, int stripe_width);
int pfs_open(const char *filename, int mode);
int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset);
int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset);
int pfs_close(int fd);
int pfs_delete(const char *filename);
int pfs_fstat(int fd, struct pfs_metadata *meta_data);
int pfs_execstat(struct pfs_execstat *execstat_data);
