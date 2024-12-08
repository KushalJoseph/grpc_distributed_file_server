#include "pfs_api.hpp"
#include "pfs_cache.hpp"
#include "pfs_metaserver/pfs_metaserver_api.hpp"
#include "pfs_fileserver/pfs_fileserver_api.hpp"
#include <grpcpp/grpcpp.h>  
#include "../pfs_proto/pfs_metaserver.pb.h"     
#include "../pfs_proto/pfs_metaserver.grpc.pb.h"  
#include "../pfs_proto/pfs_fileserver.pb.h"    
#include "../pfs_proto/pfs_fileserver.grpc.pb.h" 

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
            std::cerr << "Failed to connect to server " << server_address << ": " << status.error_message() << std::endl;
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
            std::cerr << "Failed to connect to server " << server_address << ": " << status.error_message() << std::endl;
            return false;
        }
    }
    return false;
}

std::vector<std::string> get_server_addresses() {
    // Read pfs_list.txt
    std::ifstream pfs_list("pfs_list.txt");
    if (!pfs_list.is_open()) {
        std::cerr << "Failed to open pfs_list.txt file!" << std::endl;
        return {};
    }

    // Check if all servers (NUM_FILE_SERVERS + 1) are online
    std::string server_address;
    std::vector<std::string> server_addresses;
    while (std::getline(pfs_list, server_address)) {
        server_addresses.push_back(server_address);
    }
    return server_addresses;
}

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

std::string extract_name(std::string filename) {
    size_t dot_pos = filename.find('.');
    // If no dot is found, return the entire filename
    if (dot_pos == std::string::npos) {
        return filename;
    }
    // Otherwise, return the substring up to the dot
    return filename.substr(0, dot_pos);
}

int pfs_initialize() {
    if(verify_all_servers_online() == -1){
        std::cerr << "Servers not online" << std::endl;
        return -1;
    }
    
    // Connect with metaserver using gRPC
    metaserver_api_initialize();

    std::vector<std::string> server_addresses = get_server_addresses();
    // Connect with all fileservers (NUM_FILE_SERVERS) using gRPC
    for (size_t i = 1; i < server_addresses.size(); i++) {
        fileserver_api_initialize(server_addresses[i]);
    }
    
    static int client_id = 0;
    client_id++;
    return client_id;
}

int pfs_finish(int client_id) {

    return 0;
}

int pfs_create(const char *filename, int stripe_width) {
    // get the metadata necessary to create the file(s)
    metaserver_api_create(filename, stripe_width);

    // create files (or send requests) to the appropriate file servers.
    return 1;
}

int pfs_open(const char *filename, int mode) {
    int fd = metaserver_api_open(filename, mode);
    return fd;
}

int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset) {
    // ...

    // Check client cache
    cache_func_temp();

    // ...

    std::pair<std::vector<struct Chunk>, std::string> read_instructions = metaserver_api_read(fd, buf, num_bytes, offset);
    if (read_instructions.second == "FAIL") {
        return -1;
    }
    
    std::string filename = extract_name(read_instructions.second);
    std::vector<std::string> server_addresses = get_server_addresses();

    std::string total_content = "";
    for(struct Chunk &chunk: read_instructions.first){
        std::string cur_content = "";
        std::string chunk_filename = std::to_string(chunk.server_number) + "_" + filename + "_" + std::to_string(chunk.chunk_number);
        std::cout << "Instructed to read from file called " << chunk_filename << " in File Server " << chunk.server_number << ". Start from " << chunk.start_byte << " till " << chunk.end_byte << std::endl;
        
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
        total_content += cur_content;
    }
    std::memcpy(buf, total_content.c_str(), total_content.size());
    return 0;
}

int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset) {
    // ...

    // Check client cache
    cache_func_temp();

    // ...

    std::pair<std::vector<struct Chunk>, std::string> instructions = metaserver_api_write(fd, buf, num_bytes, offset);
    if (instructions.second == "FAIL") {
        return -1;
    }
    
    std::string filename = extract_name(instructions.second);
    std::vector<std::string> server_addresses = get_server_addresses();
    for(struct Chunk &chunk: instructions.first){
        std::string chunk_filename = std::to_string(chunk.server_number) + "_" + filename + "_" + std::to_string(chunk.chunk_number);
        std::cout << "Instructed to write to file called " << chunk_filename << " to File Server " << chunk.server_number << ". Start from " << chunk.start_byte << " till " << chunk.end_byte << std::endl;
        
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
    }

    return 0;
}

int pfs_close(int fd) {
    return metaserver_api_close(fd);
}

int pfs_delete(const char *filename) {
    int m = metaserver_api_delete(filename);
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
    return metaserver_api_fstat(fd, meta_data);
}

int pfs_execstat(struct pfs_execstat *execstat_data) {

    return 0;
}
