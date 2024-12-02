#include "pfs_fileserver_api.hpp"
#include "pfs_proto/pfs_fileserver.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <vector>

std::vector<std::unique_ptr<pfsfile::PFSFileServer::Stub>> connect_to_fileservers() {
    // Step 0: Read the server's address and port from a file or configuration
    std::filesystem::path current_path = std::filesystem::current_path();

    std::ifstream config_file("pfs_list.txt");
    if (!config_file.is_open()) {
        std::cerr << "Failed to open config file!" << std::endl;
        return {};
    }

    std::string server_address;
    std::vector<std::string> server_addresses;
    while (std::getline(config_file, server_address)) {
        server_addresses.push_back(server_address);
    }
    config_file.close();

    std::vector<std::unique_ptr<pfsfile::PFSFileServer::Stub>> stubs;
    for (size_t i = 1; i <= NUM_FILE_SERVERS; i++) {
        std::string fileserver_address = server_addresses[i];
        std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(fileserver_address, grpc::InsecureChannelCredentials());
        std::unique_ptr<pfsfile::PFSFileServer::Stub> stub = pfsfile::PFSFileServer::NewStub(channel);
        stubs.push_back(std::move(stub));
    }
    return stubs;
}

void fileserver_api_initialize() {
    printf("%s: called.\n", __func__);
    auto stubs = connect_to_fileservers(); // stubs to each of the fileservers in NUM_FILESERVERS
    if (!stubs.size()) {
        std::cerr << "Failed to connect to fileservers" << std::endl;
        return;
    }
    
    for (size_t i = 0; i < stubs.size(); i++) {
        pfsfile::InitRequest request;
        pfsfile::InitResponse response;

        grpc::ClientContext context;

        grpc::Status status = stubs[i]->Initialize(&context, request, &response);
        if (status.ok()) {
            printf("Initialize RPC succeeded: %s\n", response.message().c_str());
        } else {
            fprintf(stderr, "Initialize RPC failed: %s\n", status.error_message().c_str());
        }
    }
}
