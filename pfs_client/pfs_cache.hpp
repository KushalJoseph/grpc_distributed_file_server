#pragma once

#include <cstdio>
#include <cstdlib>
#include <string>
#include <map>
#include <list>
#include <iostream>
#include <vector>

#include "pfs_common/pfs_config.hpp"
#include "pfs_api.hpp"

void cache_func_temp();

struct CachedBlock {
    std::string data;  // Cached data
    bool valid;              // Indicates if the block is valid
    CachedBlock() : valid(false) {}

    std::string to_string() const {
        if (valid) return data;
        else return "invalid block";
    }
};

class LRUCache {
    size_t max_size; 
    using RangeKey = std::pair<int, int>;

    std::map<std::string, std::map<RangeKey, CachedBlock>> cache;
    std::list<std::pair<std::string, RangeKey>> lru_list;
    // Map for quick lookup of LRU positions
    std::map<std::string, std::map<RangeKey, std::list<std::pair<std::string, RangeKey>>::iterator>> lru_positions;

public:
    LRUCache(size_t size) : max_size(size) {}

    int read(const std::string& filename, int start_byte, int end_byte, std::string &result) {
        std::cout << "Querying Cache for " << filename << " from " << start_byte << "-" << end_byte << std::endl;
        if (cache.find(filename) == cache.end()) {
            return -1; 
        }

        auto& cached_blocks = cache[filename];
        int current_start = start_byte;

        for (const auto& [range, block] : cached_blocks) {
            if (!block.valid) continue;

            int block_start = range.first;
            int block_end = range.second;

            if (block_start <= current_start && current_start <= block_end) {
                int copy_start = std::max(current_start, block_start);
                int copy_end = std::min(end_byte, block_end);

                result += block.data.substr(copy_start - block_start, copy_end - copy_start + 1);

                update_lru(filename, range);

                current_start = copy_end + 1;
                if (current_start > end_byte) {
                    break;
                }
            }
        }

        if (current_start <= end_byte) {
            std::cout << "Cache Miss" << std::endl;
            return -1; // Cache miss
        }

        return 0;
    }

    // Update cache with new data
    void update_cache(const std::string& filename, int start_byte, int end_byte, const std::string& data) {
        std::cout << "Updating Cache for " << filename << " from " << start_byte << "-" << end_byte << std::endl;
        std::cout << "Updating with content: " << data << std::endl;
        RangeKey range = {start_byte, end_byte};
        if (cache[filename].size() >= max_size) {
            evict();
        }

        CachedBlock block;
        block.data = data;
        block.valid = true;
        cache[filename][range] = block;
        update_lru(filename, range);
    }

    void invalidate(std::string filename, struct FileToken revoked_token) {
        /* Invalidate caches in the same range */
        auto& cached_blocks = cache[filename];
        std::vector<std::pair<int, int>> to_invalidate; // we will delete these cache blocks
        std::vector<std::pair<std::pair<int, int>, CachedBlock>> to_split; // we will add these new "split" blocks

        for (auto it = cached_blocks.begin(); it != cached_blocks.end(); ) {
            const auto& [block_range, block] = *it;
            int block_start = block_range.first, block_end = block_range.second;

            // Check if the block overlaps with the REVOKED TOKEN
            if (block_start <= revoked_token.end_byte && block_end >= revoked_token.start_byte) {
                std::cout << "Invalidating cache block of [" << block_start << "-" << block_end << "]\n";

                // Remove the invalidated block
                to_invalidate.push_back(block_range);

                int new_start, new_end;
                if (block_start < revoked_token.start_byte) {
                    new_start = block_start;
                    new_end = revoked_token.start_byte - 1;
                    CachedBlock split_block = block;

                    // Adjust the block's data for the new range
                    int offset = new_start - block_range.first;  // Calculate offset within the original data
                    if (new_end - new_start + 1 > 0) {
                        split_block.data = block.data.substr(offset, new_end - new_start + 1);  // Extract relevant portion of data

                        // Emplace the modified block
                        std::cout << "Adding split cache block: " << split_block.to_string() << std::endl;
                        to_split.emplace_back(std::make_pair(new_start, new_end), split_block);
                    }
                }
                if (block_end > revoked_token.end_byte) {
                    new_start = revoked_token.end_byte + 1;
                    new_end = block_end;
                    CachedBlock split_block = block;

                    // Adjust the block's data for the new range
                    int offset = new_start - block_range.first;  // Calculate offset within the original data
                    if (new_end - new_start + 1 > 0) {
                        split_block.data = block.data.substr(offset, new_end - new_start + 1);  // Extract relevant portion of data

                        // Emplace the modified block
                        std::cout << "Adding split cache block: " << split_block.to_string() << std::endl;
                        to_split.emplace_back(std::make_pair(new_start, new_end), split_block);
                    }
                }
            }
            ++it; // Continue iteration
        }

        // Remove invalidated blocks
        for (const auto& range : to_invalidate) {
            cached_blocks.erase(range);
        }

        // Add the split blocks
        for (const auto& [new_range, block] : to_split) {
            cached_blocks[new_range] = block;
            std::cout << "Added new cache block [" << new_range.first << "-" << new_range.second << "]\n";
        }
    }

private:
    void update_lru(const std::string& filename, const RangeKey& range) {
        auto& file_positions = lru_positions[filename];

        if (file_positions.find(range) != file_positions.end()) {
            lru_list.erase(file_positions[range]);
        }

        lru_list.emplace_front(filename, range);
        file_positions[range] = lru_list.begin();

        if (lru_list.size() > max_size) {
            evict();
        }
    }

    void evict() {
        if (lru_list.empty()) return;

        auto [filename, range] = lru_list.back();
        lru_list.pop_back();
        lru_positions[filename].erase(range);

        if (lru_positions[filename].empty()) {
            lru_positions.erase(filename);
        }

        cache[filename].erase(range);
        if (cache[filename].empty()) {
            cache.erase(filename);
        }
    }
};

int cache_api_initialize(int lru_size);

int cache_api_read(std::string filename, int start_byte, int end_byte, std::string &result);

void cache_api_update(std::string filename, int start_byte, int end_byte, std::string data);

void cache_api_invalidate(std::string filename, struct FileToken revoked_token);

