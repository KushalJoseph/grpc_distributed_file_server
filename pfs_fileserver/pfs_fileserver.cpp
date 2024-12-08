#include "pfs_fileserver.hpp"
#include "../pfs_proto/pfs_fileserver.grpc.pb.h"
#include "../pfs_proto/pfs_fileserver.pb.h"
#include "../pfs_client/pfs_api.hpp"
#include <grpcpp/grpcpp.h>
#include <fstream>
#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <cassert>
#include <filesystem>
#include <regex>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using namespace pfsfile;

// Define the gRPC service implementation
class PFSFileServerImpl final : public PFSFileServer::Service {
private:
    void writeToLocalFile(const std::string& filename, 
                        const std::pair<int, int>& range_within_buffer,
                        const std::pair<int, int>& range_within_local_file,
                        const std::string& buffer) {
        // Assert that the range sizes are equal
        assert((range_within_buffer.second - range_within_buffer.first) == 
            (range_within_local_file.second - range_within_local_file.first));

        std::string file_path = "./files/" + filename;

        std::filesystem::create_directories("./files");
        std::cout << "Writing buf[" << range_within_buffer.first << " - " << range_within_buffer.second 
                << "] to local " << file_path << ", starting from " << range_within_local_file.first 
                << ", till " << range_within_local_file.second << std::endl;

        // Open the file in read/write mode
        std::fstream file(file_path, std::ios::in | std::ios::out | std::ios::binary);

        if (!file.is_open()) {
            // If the file doesn't exist, create it and open it
            file.open(file_path, std::ios::out | std::ios::binary);
            if (!file.is_open()) {
                std::cerr << "Error opening file: " << file_path << std::endl;
                return;
            }
        }

        std::cout << "File is ready to write to " << std::endl;
        // Get the current size of the file
        file.seekg(0, std::ios::end);
        int current_file_size = file.tellg();

        // If the file's current size is less than the starting position, extend the file
        if (current_file_size < range_within_local_file.first) {
            file.seekp(range_within_local_file.first - 1, std::ios::beg);
            file.write("\0", 1);  // Write a single null byte to extend the file
        }

        // Now write the buffer to the file at the appropriate location
        file.seekp(range_within_local_file.first, std::ios::beg);
        file.write(buffer.substr(range_within_buffer.first, range_within_buffer.second - range_within_buffer.first + 1).c_str(), 
                range_within_buffer.second - range_within_buffer.first + 1);

        // Close the file after writing
        file.close();
    }

    void readFromLocalFile(const std::string& filename,  
                        const std::pair<int, int>& range_within_local_file,
                        std::string& buffer) {
        std::string file_path = "./files/" + filename;

        std::filesystem::create_directories("./files");
        std::cout << "Reading from local " << file_path << ", starting from " << range_within_local_file.first 
                << ", till " << range_within_local_file.second << std::endl;

        // Open the file in read mode
        std::ifstream file(file_path, std::ios::in | std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Error opening file: " << file_path << std::endl;
            return;
        }

        // Calculate the size of the range to read
        int file_range_size = range_within_local_file.second - range_within_local_file.first + 1;

        // Resize the buffer to hold the data
        buffer.resize(file_range_size);

        // Move the file pointer to the starting position
        file.seekg(range_within_local_file.first, std::ios::beg);

        // Read the content from the file into the buffer
        file.read(&buffer[0], file_range_size);

        if (!file) {
            std::cerr << "Error reading file. Bytes read: " << file.gcount() << std::endl;
            buffer.clear(); // Clear buffer if reading fails
        }

        // Close the file
        file.close();
    }

public:
    Status Ping(ServerContext* context, const PingRequest* request, PingResponse* reply) override {
        reply->set_message("Thanks for the Ping. I, the fileserver am alive!");
        printf("%s: Received ping RPC call.\n", __func__);
        return Status::OK;
    }

    Status Initialize(ServerContext* context, const InitRequest* request, InitResponse* reply) override {
        reply->set_message("Connection successful! File server is active.");
        printf("%s: Received Initialize RPC call.\n", __func__);
        return Status::OK;
    }

    Status WriteFile(ServerContext* context, const WriteFileRequest* request, WriteFileResponse* reply) override {
        printf("%s: Received WriteFile RPC call.\n", __func__);

        std::string buf = request->buf();
        std::string filename = request->chunk_filename();
        int chunk_number = request->chunk_number();
        int start_byte = request->start_byte();
        int end_byte = request->end_byte();
        int num_bytes = request->num_bytes();
        int offset = request->offset();
        // assert num bytes

        std::pair<int, int> range_within_local_file = {start_byte - chunk_number * 1024, end_byte - chunk_number * 1024};
        std::pair<int, int> range_within_buffer = {start_byte - offset, end_byte - offset};

        assert(range_within_local_file.second - range_within_local_file.first == range_within_buffer.second - range_within_buffer.first);
        writeToLocalFile(filename, range_within_buffer, range_within_local_file, buf);

        std::string msgToSend = "Writing local " + filename + ", starting from " + std::to_string(start_byte) + ", till " + std::to_string(end_byte) + ", total bytes: " + std::to_string(buf.size());
        reply->set_message(msgToSend);
        reply->set_bytes_written(-1); // write code
        return Status::OK;
    }

    Status ReadFile(ServerContext* context, const ReadFileRequest* request, ReadFileResponse* reply) override {
        printf("%s: Received ReadFile RPC call.\n", __func__);

        std::string filename = request->chunk_filename();
        int chunk_number = request->chunk_number();
        int start_byte = request->start_byte();
        int end_byte = request->end_byte();
        int num_bytes = request->num_bytes();
        int offset = request->offset();
        // assert num bytes

        std::pair<int, int> range_within_local_file = {start_byte - chunk_number * 1024, end_byte - chunk_number * 1024};
        
        std::string buf = "";
        readFromLocalFile(filename, range_within_local_file, buf);

        std::string msgToSend = "Reading from local " + filename + ", starting from " + std::to_string(start_byte) + ", till " + std::to_string(end_byte) + ", total bytes: " + std::to_string(buf.size());
        reply->set_content(buf);
        reply->set_message(msgToSend);
        reply->set_bytes_read(-1); // write code
        return Status::OK;
    }

    Status DeleteFile(ServerContext* context, const DeleteFileRequest* request, DeleteFileResponse* reply) override {
        printf("%s: Received DeleteFile RPC call.\n", __func__);

        std::string filename = request->filename();
        int fileserver_number = request->fileserver_number();

        try {
            std::string base_path = "./files";
            // Construct the pattern for matching files
            std::string pattern = std::to_string(fileserver_number) + "_" + filename + "_" + ".*";
            std::regex file_regex(pattern);

            // Iterate through files in the given directory
            for (const auto& entry : std::filesystem::directory_iterator(base_path)) {
                const std::string file_path = entry.path().string();
                const std::string file_name = entry.path().filename().string();
                // Check if the file matches the pattern
                if (std::regex_match(file_name, file_regex)) {
                    std::filesystem::remove(entry.path()); // Delete the file
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }

        std::string msgToSend = "Deleted " + filename;
        reply->set_message(msgToSend);
        reply->set_status_code(0);
        return Status::OK;
    }
};

void RunGRPCServer(const std::string& listen_port) {
    PFSFileServerImpl service;
    std::string server_address = "0.0.0.0:" + listen_port;

    // Build and start the server
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    printf("PFS File Server listening on %s\n", server_address.c_str());
    server->Wait();
}

int main(int argc, char *argv[]) {
    printf("%s:%s: PFS file server start! Hostname: %s, IP: %s\n",
           __FILE__, __func__, getMyHostname().c_str(), getMyIP().c_str());

    // Parse pfs_list.txt
    std::ifstream pfs_list("../pfs_list.txt");
    if (!pfs_list.is_open()) {
        fprintf(stderr, "%s: can't open pfs_list.txt file.\n", __func__);
        exit(EXIT_FAILURE);
    }

    bool found = false;
    std::string line;
    std::getline(pfs_list, line); // First line is the meta server
    while (std::getline(pfs_list, line)) {
        if (line.substr(0, line.find(':')) == getMyHostname()) {
            found = true;
            break;
        }
    }
    if (!found) {
        fprintf(stderr, "%s: hostname not found in pfs_list.txt.\n", __func__);
        exit(EXIT_FAILURE);
    }
    pfs_list.close();
    std::string listen_port = line.substr(line.find(':') + 1);

    // Run the PFS fileserver and listen to requests
    printf("%s: Launching PFS file server on %s, with listen port %s...\n",
           __func__, getMyHostname().c_str(), listen_port.c_str());

    // Do something...
    RunGRPCServer(listen_port);
    
    printf("%s:%s: PFS file server done!\n", __FILE__, __func__);
    return 0;
}
