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

struct FileToken {
    int start_byte;
    int end_byte;
    int type; // 1 = READ, 2 = WRITE
    int client_id;

    std::string to_string() const {
        std::ostringstream oss;
        oss << "start_byte: " << start_byte << "; "
            << "end_byte: " << end_byte << "; "
            << "type: " << (type == 1 ? "READ" : (type == 2 ? "WRITE" : "UNKNOWN")) << "; "
            << "client_id: " << client_id
            << "\n";
        return oss.str();
    }

    bool overlaps(const FileToken& other) const {
        return (other.end_byte >= start_byte) && (end_byte >= other.start_byte);
    }

    std::vector<FileToken> subtract(const FileToken& other) const {
        std::vector<FileToken> result;

        if (start_byte <= other.start_byte) {
            result.push_back({start_byte, other.start_byte - 1, type, client_id});
        }
        if (end_byte >= other.end_byte) {
            result.push_back({other.end_byte + 1, end_byte, type, client_id});
        }
        return result;
    }

    bool operator<(const FileToken& other) const {
        // Compare based on start_byte first, then end_byte, type, and client_id
        if (start_byte != other.start_byte) return start_byte < other.start_byte;
        if (end_byte != other.end_byte) return end_byte < other.end_byte;
        if (type != other.type) return type < other.type;
        return client_id < other.client_id;
    }
};

// for every file, define std::vector<Chunk>
struct Chunk {
    int chunk_number;
    int server_number;
    int start_byte;
    int end_byte;

    std::string to_string() const {
        std::ostringstream oss;
        oss << "chunk_number: " << chunk_number << "; ";
        oss << "server_number: " << server_number << "; ";
        oss << "byte_range: (" << start_byte << ", " << end_byte << ")\n";
        return oss.str();
    }
};

struct pfs_filerecipe {
    int stripe_width;
    std::vector<struct Chunk> chunks; // metadata.recipe.chunks

    std::string to_string() const {
        std::ostringstream oss;
        oss << "pfs_filerecipe { \n";
        oss << "stripe_width: " << stripe_width << ", \n";
        oss << "chunks: [\n";
        for (size_t i = 0; i < chunks.size(); ++i) {
            oss << chunks[i].to_string();
            if (i < chunks.size() - 1) {
                oss << "\n";
            }
        }
        oss << "] }\n";
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
