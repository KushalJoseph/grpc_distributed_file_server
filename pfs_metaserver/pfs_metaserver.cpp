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

// Define the gRPC service implementation
class PFSMetadataServerImpl final : public PFSMetadataServer::Service {
private:
    std::unordered_map<std::string, pfs_filerecipe> files;

public:
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

        pfs_filerecipe recipe;
        recipe.distribution.resize(NUM_FILE_SERVERS); // Initialize distribution with NUM_FILE_SERVERS and fill with {0, 0} indicating an empty file
        for (int i=0; i<NUM_FILE_SERVERS; i++) {
            if(i < stripe_width) recipe.distribution[i] = {0, 0};
            else recipe.distribution[i] = {-1, -1}; // don't use this file server
        }
        files[filename] = recipe;

        std::string replyMsg = "File " + filename + " created successfully. The current recipe distribution is: ";
        reply->set_message(replyMsg);
        reply->set_status_code(0);  
        return Status::OK;
    }
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
