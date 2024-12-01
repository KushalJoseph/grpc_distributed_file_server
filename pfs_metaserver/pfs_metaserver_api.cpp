#include "pfs_metaserver_api.hpp"
#include "pfs_proto/pfs_metaserver.grpc.pb.h"
#include <grpcpp/grpcpp.h>

std::unique_ptr<pfsmeta::PFSMetadataServer::Stub> connect_to_metaserver() {
    // Step 0: Read the server's address and port from a file or configuration
    std::filesystem::path current_path = std::filesystem::current_path();
    std::cout << "Current working directory: " << current_path << std::endl;

    std::ifstream config_file("pfs_list.txt");
    if (!config_file.is_open()) {
        std::cerr << "Failed to open config file!" << std::endl;
        return nullptr;
    }
    std::string server_address;
    std::getline(config_file, server_address);
    config_file.close();

    // Step 1: Create a gRPC channel to the metadata server.
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
    std::unique_ptr<pfsmeta::PFSMetadataServer::Stub> stub = pfsmeta::PFSMetadataServer::NewStub(channel);

    return stub;
}

void metaserver_api_initialize() {
    printf("%s: called.\n", __func__);
    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return;
    }
    
    // Step 2: Create a request and response object for Initialize
    pfsmeta::InitRequest request;
    pfsmeta::InitResponse response;

    // Step 3: Set up the context for the call
    grpc::ClientContext context;

    // Step 4: Make the RPC call to Initialize
    grpc::Status status = stub->Initialize(&context, request, &response);

    // Step 5: Handle the response from the metadata server
    if (status.ok()) {
        printf("Initialize RPC succeeded: %s\n", response.message().c_str());
    } else {
        fprintf(stderr, "Initialize RPC failed: %s\n", status.error_message().c_str());
    }
}

void metaserver_api_create(const char *filename, int stripe_width) {
    printf("%s: called to create file.\n", __func__);

    // Step 2: Create the connection to the metadata server using the modularized function
    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return;
    }

    // Step 3: Create a request and response object for CreateFile
    pfsmeta::CreateFileRequest request; pfsmeta::CreateFileResponse response;
    request.set_filename(filename);
    request.set_stripe_width(stripe_width);

    // Step 4: Set up the context for the call
    grpc::ClientContext context;

    // Step 5: Make the RPC call to CreateFile
    grpc::Status status = stub->CreateFile(&context, request, &response);

    // Step 6: Handle the response from the metadata server
    if (status.ok()) {
        printf("CreateFile RPC succeeded: %s\n", response.message().c_str());
    } else {
        fprintf(stderr, "CreateFile RPC failed: %s\n", status.error_message().c_str());
    }
}
