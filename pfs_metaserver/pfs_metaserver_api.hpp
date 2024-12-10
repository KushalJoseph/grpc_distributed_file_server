#pragma once

#include <cstdio>
#include <cstdlib>

#include "pfs_common/pfs_config.hpp"
#include "pfs_common/pfs_common.hpp"
#include "pfs_client/pfs_api.hpp"

int metaserver_api_initialize();

void metaserver_api_create(const char *filename, int stripe_width, int client_id);

int metaserver_api_open(const char *filename, int mode, int client_id);

int metaserver_api_close(int mode, int client_id);

int metaserver_api_fstat(int fd, struct pfs_metadata *meta_data, int client_id);

int metaserver_api_delete(const char *filename, int client_id);

void metaserver_api_request_token(int fd, int start_byte, int end_byte, int mode, int client_id);

bool metaserver_api_check_tokens(int fd, int start_byte, int end_byte, int mode, int client_id);

/* <instructions, filename */
std::pair<std::vector<struct Chunk>, std::string> metaserver_api_write(int fd, const void *buf, size_t num_bytes, off_t offset, int client_id);

/* <instructions, filename */
std::pair<std::vector<struct Chunk>, std::string> metaserver_api_read(int fd, const void *buf, size_t num_bytes, off_t offset, int client_id);
