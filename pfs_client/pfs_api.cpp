#include "pfs_api.hpp"
#include "pfs_cache.hpp"
#include "pfs_metaserver/pfs_metaserver_api.hpp"
#include "pfs_fileserver/pfs_fileserver_api.hpp"

// bool is_server_online(const std::string& server_address) {
//     std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
//     std::unique_ptr<pfsmeta::PFSMetadataServer::Stub> stub = pfsmeta::PFSMetadataServer::NewStub(channel);

//     // Create a simple request to check the server's availability
//     pfsmeta::InitRequest request;
//     pfsmeta::InitResponse response;
//     grpc::ClientContext context;

//     // Perform a simple RPC call (e.g., Initialize) to check if the server is responsive
//     grpc::Status status = stub->Initialize(&context, request, &response);
    
//     if (status.ok()) {
//         std::cout << "Server " << server_address << " is online." << std::endl;
//         return true;
//     } else {
//         std::cerr << "Failed to connect to server " << server_address << ": " << status.error_message() << std::endl;
//         return false;
//     }
// }

void verify_all_servers_online() {
    // // Read pfs_list.txt
    // std::ifstream pfs_list("pfs_list.txt");
    // if (!pfs_list.is_open()) {
    //     std::cerr << "Failed to open pfs_list.txt file!" << std::endl;
    //     return -1;
    // }

    // // Check if all servers (NUM_FILE_SERVERS + 1) are online
    // std::string server_address;
    // std::vector<std::string> server_addresses;

    // // Read all server addresses from the file
    // while (std::getline(pfs_list, server_address)) {
    //     server_addresses.push_back(server_address);
    // }

    // // Step 2: Check if all servers (NUM_FILE_SERVERS + the metaserver) are online
    // if (server_addresses.empty()) {
    //     std::cerr << "No servers listed in pfs_list.txt!" << std::endl;
    //     return -1;
    // }

    // std::cout << "Checking if servers are online..." << std::endl;

    // // Check the metaserver (first server in the list)
    // std::string metaserver_address = server_addresses[0];
    // if (!is_server_online(metaserver_address)) {
    //     std::cerr << "Metadata server is not online!" << std::endl;
    //     return -1;
    // }

    // for (size_t i = 1; i < server_addresses.size(); ++i) {
    //     std::string fileserver_address = server_addresses[i];
    //     if (!is_server_online(fileserver_address)) {
    //         std::cerr << "File server " << fileserver_address << " is not online!" << std::endl;
    //         std::cout << "DOING NOTHING FOR NOW" << std::endl;
    //         // return -1;
    //     }
    // }

    // // All servers are online
    // std::cout << "All servers are online!" << std::endl;
}

int pfs_initialize() {
    verify_all_servers_online();
    
    // Connect with metaserver using gRPC
    metaserver_api_initialize();

    // Connect with all fileservers (NUM_FILE_SERVERS) using gRPC
    // for (int i = 0; i < NUM_FILE_SERVERS; ++i) {
    //     fileserver_api_temp();
    // }

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

    return 0;
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
