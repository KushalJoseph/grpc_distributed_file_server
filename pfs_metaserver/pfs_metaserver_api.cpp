#include "pfs_metaserver_api.hpp"
#include "pfs_proto/pfs_metaserver.grpc.pb.h"
#include "pfs_client/pfs_api.hpp"
#include <grpcpp/grpcpp.h>

std::unique_ptr<pfsmeta::PFSMetadataServer::Stub> connect_to_metaserver() {
    // Step 0: Read the server's address and port from a file or configuration
    std::filesystem::path current_path = std::filesystem::current_path();

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
    
    pfsmeta::InitRequest request;
    pfsmeta::InitResponse response;

    grpc::ClientContext context;

    grpc::Status status = stub->Initialize(&context, request, &response);
    if (status.ok()) {
        printf("Initialize RPC succeeded: %s\n", response.message().c_str());
    } else {
        fprintf(stderr, "Initialize RPC failed: %s\n", status.error_message().c_str());
    }
}

void metaserver_api_create(const char *filename, int stripe_width) {
    printf("%s: called to create file.\n", __func__);

    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return;
    }

    pfsmeta::CreateFileRequest request; pfsmeta::CreateFileResponse response;
    request.set_filename(filename);
    request.set_stripe_width(stripe_width);

    grpc::ClientContext context;

    grpc::Status status = stub->CreateFile(&context, request, &response);
    if (status.ok()) {
        printf("CreateFile RPC succeeded: %s\n", response.message().c_str());
    } else {
        fprintf(stderr, "CreateFile RPC failed: %s\n", status.error_message().c_str());
    }
}

int metaserver_api_open(const char *filename, int mode) {
    printf("%s: called to open file.\n", __func__);

    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return -1;
    }

    pfsmeta::OpenFileRequest request; pfsmeta::OpenFileResponse response;
    request.set_filename(filename);
    request.set_mode(mode);

    grpc::ClientContext context;

    grpc::Status status = stub->OpenFile(&context, request, &response);
    if (status.ok()) {
        printf("OpenFile RPC succeeded: %s\n", response.message().c_str());
        return response.file_descriptor();
    } else {
        fprintf(stderr, "OpenFile RPC failed: %s\n", status.error_message().c_str());
    }
    return -1;
}

int metaserver_api_close(int file_descriptor) {
    printf("%s: called to close file.\n", __func__);

    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return -1;
    }

    pfsmeta::CloseFileRequest request; pfsmeta::CloseFileResponse response;
    request.set_file_descriptor(file_descriptor);

    grpc::ClientContext context;

    grpc::Status status = stub->CloseFile(&context, request, &response);
    if (status.ok()) {
        printf("CloseFile RPC succeeded: %s\n", response.message().c_str());
        return 0;
    } else {
        fprintf(stderr, "CreateFile RPC failed: %s\n", status.error_message().c_str());
    }
    return -1;
}

std::pair<std::vector<struct Chunk>, std::string> metaserver_api_write(int fd, const void *buf, size_t num_bytes, off_t offset) {
    printf("%s: called to write to file.\n", __func__);

    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return {{}, "FAIL"};
    }

    pfsmeta::WriteToFileRequest request; pfsmeta::WriteToFileResponse response;
    request.set_file_descriptor(fd);

    std::string bytes_string(static_cast<const char*>(buf), num_bytes);
    request.set_buf(bytes_string);
    request.set_num_bytes(num_bytes);
    request.set_offset(offset);

    grpc::ClientContext context;

    grpc::Status status = stub->WriteToFile(&context, request, &response);
    if (status.ok()) {
        printf("WriteFile RPC succeeded: %s\n", response.message().c_str());

        std::vector<struct Chunk> instructions;
        for (const auto& instruction: response.instructions()) {
            Chunk chunk;
            chunk.chunk_number = instruction.chunk_number();
            chunk.server_number = instruction.server_number();
            chunk.start_byte = instruction.start_byte();
            chunk.end_byte = instruction.end_byte();
            instructions.push_back(chunk);
        }
        std::cout << "I have received the instructions from server" << std::endl;
        return {instructions, response.filename()};
    } else {
        fprintf(stderr, "WriteToFile RPC failed: %s\n", status.error_message().c_str());
    }
    return {{}, "FAIL"};
}

std::pair<std::vector<struct Chunk>, std::string> metaserver_api_read(int fd, const void *buf, size_t num_bytes, off_t offset) {
    printf("%s: called to read from file.\n", __func__);

    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return {{}, "FAIL"};
    }

    pfsmeta::ReadFileRequest request; pfsmeta::ReadFileResponse response;
    request.set_file_descriptor(fd);

    std::string bytes_string(static_cast<const char*>(buf), num_bytes);
    request.set_buf(""); // empty buffer
    request.set_num_bytes(num_bytes);
    request.set_offset(offset);

    grpc::ClientContext context;

    grpc::Status status = stub->ReadFile(&context, request, &response);
    if (status.ok()) {
        printf("ReadFile RPC succeeded: %s\n", response.message().c_str());

        std::vector<struct Chunk> instructions;
        for (const auto& instruction: response.instructions()) {
            Chunk chunk;
            chunk.chunk_number = instruction.chunk_number();
            chunk.server_number = instruction.server_number();
            chunk.start_byte = instruction.start_byte();
            chunk.end_byte = instruction.end_byte();
            instructions.push_back(chunk);
        }
        std::cout << "I have received the instructions from server" << std::endl;
        return {instructions, response.filename()};
    } else {
        fprintf(stderr, "WriteToFile RPC failed: %s\n", status.error_message().c_str());
    }
    return {{}, "FAIL"};
}


