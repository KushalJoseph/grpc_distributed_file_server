#include "pfs_metaserver.hpp"
#include "../pfs_proto/pfs_metaserver.grpc.pb.h"
#include "../pfs_proto/pfs_metaserver.pb.h"
#include "../pfs_client/pfs_api.hpp"
#include <grpcpp/grpcpp.h>
#include <fstream>
#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using namespace pfsmeta;

const int MODE_READ = 1;
const int MODE_WRITE = 2;

class PFSMetadataServerImpl final : public PFSMetadataServer::Service {
private:
    std::unordered_map<std::string, pfs_metadata> files;
    std::unordered_map<int, std::pair<std::string, int>> descriptor;
    std::unordered_map<std::string, int> fileNameToDescriptor;
    
    int next_fd = 3;
    std::set<int> used_fds;

public:
    Status Ping(ServerContext* context, const PingRequest* request, PingResponse* reply) override {
        reply->set_message("Thanks for the Ping. I, the metaserver am alive!");
        printf("%s: Received ping RPC call.\n", __func__);
        return Status::OK;
    }

    Status Initialize(ServerContext* context, const InitRequest* request, InitResponse* reply) override {
        reply->set_message("Connection successful! Metadata server is active.");
        printf("%s: Received Initialize RPC call.\n", __func__);
        return Status::OK;
    }

    Status CreateFile(ServerContext* context, const pfsmeta::CreateFileRequest* request, pfsmeta::CreateFileResponse* reply) override {        
        std::string filename = request->filename();
        int stripe_width = request->stripe_width();
        if(stripe_width > NUM_FILE_SERVERS) {
            std::string msg = "Stripe width cannot exceed the number of file servers!";
            std::cerr << msg << std::endl;
            reply->set_message(msg);
            reply->set_status_code(1);
            return Status(grpc::StatusCode::INVALID_ARGUMENT, msg);
        }

        pfs_metadata file_metadata;
        pfs_filerecipe recipe;
        recipe.stripe_width = stripe_width;
        recipe.distribution = std::vector<std::vector<int>> (NUM_FILE_SERVERS, std::vector<int> (2)); // Initialize distribution with NUM_FILE_SERVERS and fill with {0, 0} indicating an empty file
        for (int i=0; i<NUM_FILE_SERVERS; i++) {
            if(i < stripe_width) recipe.distribution[i][0] = 0, recipe.distribution[i][1] = 0;
            else recipe.distribution[i][0] = -1, recipe.distribution[i][1] = -1; // don't use this file server
        }        

        std::strncpy(file_metadata.filename, filename.c_str(), sizeof(file_metadata.filename) - 1);
        file_metadata.filename[sizeof(file_metadata.filename) - 1] = '\0'; // Ensure null termination
        file_metadata.file_size = 0;
        file_metadata.recipe = recipe;

        // creation, updation time
        files[filename] = file_metadata;

        std::string replyMsg = "File " + filename + " created successfully.\n" + (files[filename].to_string());
        reply->set_message(replyMsg);
        reply->set_status_code(0);  
        return Status::OK;
    }

    Status OpenFile(ServerContext* context, const OpenFileRequest* request, OpenFileResponse* reply) override {
        std::string filename = request->filename();

        if(files.find(filename) == files.end()) {
            std::string msg = "File does not exist!";
            std::cerr << msg << std::endl;
            reply->set_message(msg);
            reply->set_status_code(1);
            return Status(grpc::StatusCode::INVALID_ARGUMENT, msg);
        }
        if(fileNameToDescriptor.find(filename) != fileNameToDescriptor.end()){
            int fd = fileNameToDescriptor[filename];
            if(descriptor[fd].second == 2) {
                std::string msg = "File is being written to ";
                std::cerr << msg << std::endl;
                reply->set_message(msg);
                reply->set_status_code(0);
                return Status(grpc::StatusCode::INVALID_ARGUMENT, msg);
            }
        }
        
        int mode = request->mode();
        if (mode == MODE_READ) {
            int fd = next_fd++;
            used_fds.insert(fd);

            reply->set_message("File opened for you");
            reply->set_file_descriptor(fd);
            printf("%s: Received Initialize RPC call.\n", __func__);

            descriptor[fd] = {filename, MODE_READ};
            fileNameToDescriptor[filename] = fd;
            return Status::OK;
        } else if (mode == MODE_WRITE) {
            int fd = next_fd++;
            used_fds.insert(fd);

            reply->set_message("File opened for you");
            reply->set_file_descriptor(fd);
            printf("%s: Received Initialize RPC call.\n", __func__);

            descriptor[fd] = {filename, MODE_WRITE};
            fileNameToDescriptor[filename] = fd;
            return Status::OK;
        }
    }

    Status CloseFile(ServerContext* context, const pfsmeta::CloseFileRequest* request, pfsmeta::CloseFileResponse* reply) override {        
        int fd = request->file_descriptor();
        if (descriptor.find(fd) == descriptor.end()) {
            std::string msg = "File may already be closed!";
            std::cerr << msg << std::endl;
            reply->set_message(msg);
            reply->set_status_code(0);
            return Status::OK;
        }
        std::string filename = descriptor[fd].first;

        fileNameToDescriptor.erase(filename);
        descriptor.erase(fd);

        std::string replyMsg = "File " + filename + " closed successfully.\n";
        reply->set_message(replyMsg);
        reply->set_status_code(0);  
        return Status::OK;
    }

    // Status WriteToFile(ServerContext* context, const pfsmeta::WriteToFileRequest* request, pfsmeta::WriteToFileResponse* reply) override {        
    //     std::string filename = request->filename();
    //     if (files.find(filename) == files.end()) {
    //         std::cerr << "No file with " << filename << " exists!" << std::endl;
    //         return Status(grpc::StatusCode::INVALID_ARGUMENT, msg);
    //     }
    //     int numBytesNew = request->numBytes();
    //     int numBytesOld = files[filename].file_size;

    //     pfs_filerecipe recipe = files[filename];
    //     int stripe_width = recipe.stripe_width;
        

    //     int bytes_per_block = PFS_BLOCK_SIZE * STRIPE_BLOCKs;


    //     std::string replyMsg = "File " + filename + " created successfully. The current recipe distribution is: ";
    //     reply->set_message(replyMsg);
    //     reply->set_status_code(0);  
    //     return Status::OK;
    // }
};

void RunGRPCServer(const std::string& listen_port) {
    PFSMetadataServerImpl service;
    std::string server_address = "0.0.0.0:" + listen_port;

    // Build and start the server
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    printf("PFS Metadata Server listening on %s\n", server_address.c_str());
    server->Wait();
}

int main(int argc, char *argv[]) {
    printf("%s:%s: PFS meta server start! Hostname: %s, IP: %s\n", __FILE__,
           __func__, getMyHostname().c_str(), getMyIP().c_str());

    // Parse pfs_list.txt
    std::ifstream pfs_list("../pfs_list.txt");
    if (!pfs_list.is_open()) {
        fprintf(stderr, "%s: can't open pfs_list.txt file.\n", __func__);
        exit(EXIT_FAILURE);
    }

    std::string line;
    std::getline(pfs_list, line);
    if (line.substr(0, line.find(':')) != getMyHostname()) {
        fprintf(stderr, "%s: hostname not on the first line of pfs_list.txt.\n",
                __func__);
        exit(EXIT_FAILURE);
    }
    pfs_list.close();
    std::string listen_port = line.substr(line.find(':') + 1);

    // Run the PFS metadata server and listen to requests
    printf("%s: Launching PFS metadata server on %s, with listen port %s...\n",
           __func__, getMyHostname().c_str(), listen_port.c_str());

    // Do something...
    RunGRPCServer(listen_port);

    printf("%s:%s: PFS meta server done!\n", __FILE__, __func__);
    return 0;
}
