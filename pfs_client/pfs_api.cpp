#include "pfs_api.hpp"
#include "pfs_cache.hpp"
#include "pfs_metaserver/pfs_metaserver_api.hpp"
#include "pfs_fileserver/pfs_fileserver_api.hpp"
#include <grpcpp/grpcpp.h>  
#include "../pfs_proto/pfs_metaserver.pb.h"     
#include "../pfs_proto/pfs_metaserver.grpc.pb.h"  
#include "../pfs_proto/pfs_fileserver.pb.h"    
#include "../pfs_proto/pfs_fileserver.grpc.pb.h" 

/* fd --> filename */
std::unordered_map<int, std::string> fd_to_filename;
int my_client_id;

/* Given a server address, checks if it's online */
bool is_server_online(const std::string& server_address, std::string serverType) {
    if (serverType == "meta") {
        std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
        std::unique_ptr<pfsmeta::PFSMetadataServer::Stub> metadataStub = pfsmeta::PFSMetadataServer::NewStub(channel);

        pfsmeta::PingRequest request;
        pfsmeta::PingResponse response;
        grpc::ClientContext context;

        grpc::Status status = metadataStub->Ping(&context, request, &response);
        if (status.ok()) {
            std::cout << serverType << " Server " << server_address << " is online." << std::endl;
            return true;
        } else {
            std::cerr << "Failed to connect to metaserver " << server_address << ": " << status.error_message() << std::endl;
            return false;
        }
    } else if (serverType == "file") {
        std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
        std::unique_ptr<pfsfile::PFSFileServer::Stub> fileserverStub = pfsfile::PFSFileServer::NewStub(channel);

        pfsfile::PingRequest request;
        pfsfile::PingResponse response;
        grpc::ClientContext context;

        grpc::Status status = fileserverStub->Ping(&context, request, &response);
        if (status.ok()) {
            std::cout << serverType << " Server " << server_address << " is online." << std::endl;
            return true;
        } else {
            std::cerr << "Failed to connect to fileserver " << server_address << ": " << status.error_message() << std::endl;
            return false;
        }
    }
    return false;
}

/* Reads pfs_list.txt and returns ALL addresses in a vector<string> */
std::vector<std::string> get_server_addresses() {
    std::ifstream pfs_list("pfs_list.txt");
    if (!pfs_list.is_open()) {
        std::cerr << "Failed to open pfs_list.txt file!" << std::endl;
        return {};
    }

    std::string server_address;
    std::vector<std::string> server_addresses;
    while (std::getline(pfs_list, server_address)) {
        server_addresses.push_back(server_address);
    }
    return server_addresses;
}

/* Uses above 2 functions to verify all servers (1 + NUM_FILE_SERVERS) are online */
int verify_all_servers_online() {
    std::vector<std::string> server_addresses = get_server_addresses();
    if (server_addresses.empty()) {
        std::cerr << "No servers listed in pfs_list.txt!" << std::endl;
        return -1;
    }

    std::cout << "Checking if servers are online..." << std::endl;

    std::string metaserver_address = server_addresses[0];
    if (!is_server_online(metaserver_address, "meta")) {
        std::cerr << "Metadata server is not online!" << std::endl;
        return -1;
    }
    for (size_t i = 1; i <= NUM_FILE_SERVERS; i++) {
        std::string fileserver_address = server_addresses[i];
        if (!is_server_online(fileserver_address, "file")) {
            std::cerr << "File server " << fileserver_address << " is not online!" << std::endl;
            return -1;
        }
    }
    std::cout << "All servers are online!" << std::endl;
    return 0;
}

/* Returns name of a file without extension */
std::string extract_name(std::string filename) {
    size_t dot_pos = filename.find('.');
    if (dot_pos == std::string::npos) return filename;
    return filename.substr(0, dot_pos);
}

int pfs_initialize() {
    if(verify_all_servers_online() == -1){
        std::cerr << "Servers not online" << std::endl;
        return -1;
    }
    cache_api_initialize(PFS_BLOCK_SIZE * CLIENT_CACHE_BLOCKS);
    
    // Connect with metaserver using gRPC
    int ret = metaserver_api_initialize();
    if (ret == -1) {
        std::cerr << "Failed to initialize" << std::endl;
        return -1;
    } else {
        my_client_id = ret;
    }

    std::vector<std::string> server_addresses = get_server_addresses();
    // Connect with all fileservers (NUM_FILE_SERVERS) using gRPC
    for (size_t i = 1; i < NUM_FILE_SERVERS; i++) {
        fileserver_api_initialize(server_addresses[i]);
    }

    return my_client_id;
}

int pfs_finish(int client_id) {

    return 0;
}

/* I'm not communicating with file servers for creation */
int pfs_create(const char *filename, int stripe_width) {
    return metaserver_api_create(filename, stripe_width, my_client_id);
}

/* I'm not communicating with file servers for open */
int pfs_open(const char *filename, int mode) {
    int fd = metaserver_api_open(filename, mode, my_client_id);
    fd_to_filename[fd] = filename;
    return fd;
}

/**
    1. Check if you have the READ token for requested bytes
    2. If not, request the token. (In the backend, you will wait until you have the token)
    3. Call metaserver to receive read instructions
    4. Send received instructions to respective fileservers, and collect the results, one by one
 */
int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset) {
    // ...

    // Check client cache
    std::string total_content = "";
    int s_byte = (int) offset, e_byte = (int) s_byte + (int) num_bytes - 1;
    int cache_hit = cache_api_read(fd_to_filename[fd], s_byte, e_byte, total_content);
    if (cache_hit != -1) {
        std::cout << "Cache Hit!" << std::endl;
        std::memcpy(buf, total_content.c_str(), total_content.size());
        total_content += '\0';
        return total_content.size();
    }

    if (!metaserver_api_check_tokens(fd, offset, offset + num_bytes - 1, 1, my_client_id)) {
        std::cout << "I don't have the read token for " << offset << "-" << offset + num_bytes - 1 << " so I'm going to request it" << std::endl;
        metaserver_api_request_token(fd, offset, offset + num_bytes - 1, 1, my_client_id); // 2 = MODE_WRITE
    } 

    std::pair<std::vector<struct Chunk>, std::string> read_instructions = metaserver_api_read(fd, buf, num_bytes, offset, my_client_id);
    if (read_instructions.second == "FAIL") {
        return -1;
    }
    
    std::string filename = extract_name(read_instructions.second);
    std::vector<std::string> server_addresses = get_server_addresses();

    total_content = "";
    int bytes_read = 0;
    for(struct Chunk &chunk: read_instructions.first){
        std::string cur_content = "";
        std::string chunk_filename = std::to_string(chunk.server_number) + "_" + filename + "_" + std::to_string(chunk.chunk_number);        
        std::string fileserver_address = server_addresses[chunk.server_number + 1]; // +1 since 0 is metaserver
        fileserver_api_read(fileserver_address, 
                                cur_content, 
                                chunk_filename, 
                                chunk.chunk_number, 
                                num_bytes, 
                                chunk.start_byte, 
                                chunk.end_byte,
                                offset
        );
        bytes_read += cur_content.size();
        total_content += cur_content;
    }
    total_content += '\0';
    std::memcpy(buf, total_content.c_str(), total_content.size());
    cache_api_update(fd_to_filename[fd], offset, offset + num_bytes - 1, total_content);
    return bytes_read;
}


int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset) {
    // ...

    // Check client cache
    cache_func_temp();

    // ...
    if (!metaserver_api_check_tokens(fd, offset, offset + num_bytes - 1, 2, my_client_id)) {
        std::cout << "I don't have the write token for " << offset << "-" << offset + num_bytes - 1 << " so I'm going to request it" << std::endl;
        metaserver_api_request_token(fd, offset, offset + num_bytes - 1, 2, my_client_id); // 2 = MODE_WRITE
        // I will definitely have the token at this point
    } 
    
    // wait for the token to come, won't take too long.
    std::pair<std::vector<struct Chunk>, std::string> instructions = metaserver_api_write(fd, buf, num_bytes, offset, my_client_id);
    if (instructions.second == "FAIL") {
        return -1;
    }
    
    std::string filename = extract_name(instructions.second);
    std::vector<std::string> server_addresses = get_server_addresses();
    int bytes_written = 0;
    for(struct Chunk &chunk: instructions.first){
        std::string chunk_filename = std::to_string(chunk.server_number) + "_" + filename + "_" + std::to_string(chunk.chunk_number);        
        std::string fileserver_address = server_addresses[chunk.server_number + 1]; // +1 since 0 is metaserver
        fileserver_api_write(fileserver_address, 
                                buf, 
                                chunk_filename, 
                                chunk.chunk_number, 
                                num_bytes, 
                                chunk.start_byte, 
                                chunk.end_byte,
                                offset
        );
        bytes_written += (chunk.end_byte - chunk.start_byte + 1);
    }
    return bytes_written;
}

int pfs_close(int fd) {
    cache_api_close(fd_to_filename[fd]);
    fd_to_filename.erase(fd);
    return metaserver_api_close(fd, my_client_id);
}

int pfs_delete(const char *filename) {
    int m = metaserver_api_delete(filename, my_client_id);
    if (m == -1) {
        std::cerr << "Failed to delete file" << std::endl;
        return -1;
    }

    std::vector<std::string> server_addresses = get_server_addresses();
    for (size_t i = 1; i < NUM_FILE_SERVERS + 1; i++) {
        std::string filename_string = extract_name(filename);
        int f = fileserver_api_delete(filename_string, server_addresses[i], i - 1);
        if (f == -1) {
            std::cerr << "Failed to delete some file chunks " << std::endl;
            return -1;
        }
    }
    return 0;
}

int pfs_fstat(int fd, struct pfs_metadata *meta_data) {
    return metaserver_api_fstat(fd, meta_data, my_client_id);
}

int pfs_execstat(struct pfs_execstat *execstat_data) {
    return cache_api_execstat(execstat_data);
}
