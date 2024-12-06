#pragma once

#include <cstdio>
#include <cstdlib>

#include "pfs_common/pfs_config.hpp"
#include "pfs_common/pfs_common.hpp"

void metaserver_api_initialize();

void metaserver_api_create(const char *filename, int stripe_width);

int metaserver_api_open(const char *filename, int mode);

int metaserver_api_close(int mode);

/* <instructions, filename */
std::pair<std::vector<struct Chunk>, std::string> metaserver_api_write(int fd, const void *buf, size_t num_bytes, off_t offset);

/* <instructions, filename */
std::pair<std::vector<struct Chunk>, std::string> metaserver_api_read(int fd, const void *buf, size_t num_bytes, off_t offset);
