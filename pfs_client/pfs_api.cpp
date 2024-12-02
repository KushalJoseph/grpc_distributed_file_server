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

int verify_all_servers_online() {
    // Read pfs_list.txt
    std::ifstream pfs_list("pfs_list.txt");
    if (!pfs_list.is_open()) {
        std::cerr << "Failed to open pfs_list.txt file!" << std::endl;
        return -1;
    }

    // Check if all servers (NUM_FILE_SERVERS + 1) are online
    std::string server_address;
    std::vector<std::string> server_addresses;
    while (std::getline(pfs_list, server_address)) {
        server_addresses.push_back(server_address);
    }

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

int pfs_initialize() {
    if(verify_all_servers_online() == -1){
        std::cerr << "Servers not online" << std::endl;
        return -1;
    }
    
    // Connect with metaserver using gRPC
    metaserver_api_initialize();

    // Connect with all fileservers (NUM_FILE_SERVERS) using gRPC
    fileserver_api_initialize();
    
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
    std::cout << "Received FD: " << fd << std::endl;
    return fd;
}

int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset) {
    // ...

    // Check client cache
    cache_func_temp();

    // ...

    return 0;
}

int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset) {
    // ...

    // Check client cache
    cache_func_temp();

    // ...

    return 0;
}

int pfs_close(int fd) {
    metaserver_api_close(fd);
    return 0;
}

int pfs_delete(const char *filename) {

    return 0;
}

int pfs_fstat(int fd, struct pfs_metadata *meta_data) {

    return 0;
}

int pfs_execstat(struct pfs_execstat *execstat_data) {

    return 0;
}
