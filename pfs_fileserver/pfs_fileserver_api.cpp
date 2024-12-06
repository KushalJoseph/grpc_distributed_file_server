#include "pfs_fileserver_api.hpp"
#include "pfs_proto/pfs_fileserver.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <vector>

std::unique_ptr<pfsfile::PFSFileServer::Stub> connect_to_fileserver(std::string fileserver_address) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(fileserver_address, grpc::InsecureChannelCredentials());
    std::unique_ptr<pfsfile::PFSFileServer::Stub> stub = pfsfile::PFSFileServer::NewStub(channel);
    return stub;
}

/* fileserver_address: GRPC stub */
std::unordered_map<std::string, std::unique_ptr<pfsfile::PFSFileServer::Stub>> server_stub_map;

void fileserver_api_initialize(std::string fileserver_address) {
    printf("%s: called.\n", __func__);
    auto stub = connect_to_fileserver(fileserver_address);
    if (!stub) {
        std::cerr << "Failed to connect to fileserver " << fileserver_address << std::endl;
        return;
    }
    
    pfsfile::InitRequest request;
    pfsfile::InitResponse response;

    grpc::ClientContext context;

    grpc::Status status = stub->Initialize(&context, request, &response);    
    if (status.ok()) {
        printf("Initialize RPC succeeded: %s\n", response.message().c_str());
    } else {
        fprintf(stderr, "Initialize RPC failed: %s\n", status.error_message().c_str());
    }
}


void fileserver_api_write(std::string fileserver_address, 
                    const void* buf, 
                    std::string chunk_filename, 
                    int chunk_number,
                    int num_bytes, 
                    int start_byte, 
                    int end_byte,
                    int offset
                ) {

    printf("%s: called.\n", __func__);
    auto stub = connect_to_fileserver(fileserver_address); // stubs to each of the fileservers in NUM_FILESERVERS
    if (!stub) {
        std::cerr << "Failed to connect to fileserver " << fileserver_address << std::endl;
        return;
    }
    
    pfsfile::WriteFileRequest request; pfsfile::WriteFileResponse response;
    std::string bytes_string(static_cast<const char*>(buf), num_bytes);
    request.set_buf(bytes_string);
    request.set_chunk_filename(chunk_filename);
    request.set_chunk_number(chunk_number);
    request.set_start_byte(start_byte);
    request.set_end_byte(end_byte);
    request.set_num_bytes(num_bytes);
    request.set_offset(offset);

    grpc::ClientContext context;

    grpc::Status status = stub->WriteFile(&context, request, &response);
    if (status.ok()) {
        printf("Write file RPC succeeded: %s\n", response.message().c_str());
    } else {
        fprintf(stderr, "Write file RPC failed: %s\n", status.error_message().c_str());
    }
}
