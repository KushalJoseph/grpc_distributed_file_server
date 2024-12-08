#pragma once

#include <cstdio>
#include <cstdlib>

#include "pfs_common/pfs_config.hpp"
#include "pfs_common/pfs_common.hpp"

void fileserver_api_initialize(std::string fileserver_address);

void fileserver_api_write(std::string fileserver_address, 
                            const void* buf, 
                            std::string chunk_filename, 
                            int chunk_number, 
                            int num_bytes, 
                            int start_byte, 
                            int end_byte, 
                            int offset                            
);

void fileserver_api_read(std::string fileserver_address, 
                            std::string &buf, 
                            std::string chunk_filename, 
                            int chunk_number, 
                            int num_bytes, 
                            int start_byte, 
                            int end_byte, 
                            int offset                            
);

int fileserver_api_delete(std::string filename, std::string server_address, int fileserver_number);
