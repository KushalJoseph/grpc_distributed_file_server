#include "pfs_cache.hpp"

void cache_func_temp() {
    printf("%s: called.\n", __func__);
}

LRUCache cache = NULL;
int cache_api_initialize(int lru_size) {
    cache = LRUCache(lru_size);
}

int cache_api_read(std::string filename, int start_byte, int end_byte, std::string &result) {
    return cache.read(filename, start_byte, end_byte, result);
}

void cache_api_update(std::string filename, int start_byte, int end_byte, std::string data) {
    return cache.update_cache(filename, start_byte, end_byte, data);
}

void cache_api_invalidate(std::string filename, struct FileToken revoked_token) {
    return cache.invalidate(filename, revoked_token);
}

void cache_api_close(std::string filename) {
    return cache.close(filename);
}

int cache_api_execstat(struct pfs_execstat *execstat_data) {
    return cache.execstat(execstat_data);
}



