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

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using namespace pfsfile;

// Define the gRPC service implementation
class PFSFileServerImpl final : public PFSFileServer::Service {
private:

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
