#include "pfs_metaserver_api.hpp"
#include "pfs_proto/pfs_metaserver.grpc.pb.h"
#include "pfs_proto/pfs_metaserver.pb.h"
#include "pfs_client/pfs_api.hpp"
#include "pfs_client/pfs_cache.hpp"
#include <grpcpp/grpcpp.h>

std::unordered_map<std::string, std::set<FileToken>> my_tokens;
std::unordered_map<int, std::string> descriptor_to_filename; // maps descriptor to filename
int this_client_id;

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

// one object like this for every pair <file, mode>
struct FileSync {
    std::mutex mtx;
    std::condition_variable cv;
    bool token_ready = false;
};
// <filename, type> --> FileSync object
std::map<std::pair<std::string, int>, FileSync> file_sync_map;

void listenForNotifications(grpc::ClientReaderWriter<pfsmeta::TokenRequest, pfsmeta::ServerNotification>* stream) {
    pfsmeta::ServerNotification notification;
    while (stream->Read(&notification)) {
        std::cout << "\n\nHave received a notification" << std::endl;
        for (auto it: my_tokens) {
            std::cout << "Tokens BEFORE for " << it.first << std::endl;
            for (auto jt: it.second) {
                std::cout << jt.start_byte << " " << jt.end_byte << " " << jt.type << " " << jt.client_id << std::endl;
            }
        }
        // Process notifications (grants or revocations)
        if (notification.has_grant()) {
            const auto& grant = notification.grant();
            std::cout << "Received token grant for filename " << grant.filename() << ": ["
                      << grant.start_byte() << "-"
                      << grant.end_byte() << "]\n";
            FileToken granted_token = {grant.start_byte(), grant.end_byte(), grant.type(), grant.client_id()};

            my_tokens[grant.filename()].insert(granted_token);

            auto& file_sync = file_sync_map[{grant.filename(), grant.type()}];
            {
                std::lock_guard<std::mutex> lock(file_sync.mtx);
                file_sync.token_ready = true;
            }
            file_sync.cv.notify_one();  
        } else if (notification.has_revocation()) {
            const auto& revocation = notification.revocation();
            std::cout << "Received token revocation for filename " << revocation.filename() << "\n";
            bool first = true;
            for (const auto& range : revocation.new_tokens()) {
                if (first) {
                    first = false;
                    FileToken revoked_token = {range.start_byte(), range.end_byte(), range.type(), this_client_id};
                    std::cout << "Revoking " << revoked_token.start_byte << " " << revoked_token.end_byte << " " << revoked_token.type << " " << revoked_token.client_id << std::endl; 

                    cache_api_invalidate(revocation.filename(), revoked_token);

                    /* Finally, invalidate the token */
                    my_tokens[revocation.filename()].erase(revoked_token);
                } else {
                    std::cout << "\nGranting Split: [" << range.start_byte() << "-" << range.end_byte() << "]\n";
                    FileToken split_token = {range.start_byte(), range.end_byte(), range.type(), this_client_id};
                    if (range.start_byte() <= range.end_byte()) {
                        std::cout << "inserting " << split_token.to_string() << std::endl;
                        my_tokens[revocation.filename()].insert(split_token);
                    }   
                }
            }
        }
        for (auto it: my_tokens) {
            std::cout << "\nTokens AFTER for " << it.first << std::endl;
            for (auto jt: it.second) {
                std::cout << "[" << jt.start_byte << "-" << jt.end_byte << "] " << jt.type << " " << jt.client_id << std::endl;
            }
        }
        std::cout << std::endl;
    }
}

std::unique_ptr<grpc::ClientReaderWriter<pfsmeta::TokenRequest, pfsmeta::ServerNotification>> stream;
void Run(int client_id) {
    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return;
    }
    grpc::ClientContext *context = new grpc::ClientContext();
    stream = stub->TokenStream(context);
    // Start a thread to listen for server notifications
    std::thread listener([raw_stream = stream.get()]() { 
        listenForNotifications(raw_stream); 
    });

    listener.detach();
}

int metaserver_api_initialize() {
    printf("%s: called.\n", __func__);
    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return -1;
    }

    pfsmeta::InitRequest request;
    pfsmeta::InitResponse response;

    grpc::ClientContext context;
    

    grpc::Status status = stub->Initialize(&context, request, &response);
    if (status.ok()) {
        printf("Initialize RPC succeeded: %s\n", response.message().c_str());

        this_client_id = response.client_id();
        Run(this_client_id);
        return this_client_id;
    } else {
        fprintf(stderr, "Initialize RPC failed: %s\n", status.error_message().c_str());
        return -1;
    }
}

int metaserver_api_create(const char *filename, int stripe_width, int client_id) {
    printf("%s: called to create file.\n", __func__);

    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return -1;
    }

    pfsmeta::CreateFileRequest request; pfsmeta::CreateFileResponse response;
    request.set_filename(filename);
    request.set_stripe_width(stripe_width);
    request.set_client_id(client_id);

    grpc::ClientContext context;

    grpc::Status status = stub->CreateFile(&context, request, &response);
    if (status.ok()) {
        printf("CreateFile RPC succeeded: %s\n", response.message().c_str());
        return 0;
    } else {
        fprintf(stderr, "CreateFile RPC failed: %s\n", status.error_message().c_str());
        return -1;
    }
}

int metaserver_api_open(const char *filename, int mode, int client_id) {
    printf("%s: called to open file.\n", __func__);

    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return -1;
    }

    pfsmeta::OpenFileRequest request; pfsmeta::OpenFileResponse response;
    request.set_filename(filename);
    request.set_mode(mode);
    request.set_client_id(client_id);

    grpc::ClientContext context;

    grpc::Status status = stub->OpenFile(&context, request, &response);
    if (status.ok()) {
        printf("OpenFile RPC succeeded: %s\n", response.message().c_str());
        int received_fd = response.file_descriptor();
        descriptor_to_filename[received_fd] = filename;
        return received_fd;
    } else {
        fprintf(stderr, "OpenFile RPC failed: %s\n", status.error_message().c_str());
    }
    return -1;
}

int metaserver_api_close(int file_descriptor, int client_id) {
    printf("%s: called to close file.\n", __func__);

    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return -1;
    }

    pfsmeta::CloseFileRequest request; pfsmeta::CloseFileResponse response;
    request.set_file_descriptor(file_descriptor);
    request.set_client_id(client_id);

    grpc::ClientContext context;

    grpc::Status status = stub->CloseFile(&context, request, &response);
    if (status.ok()) {
        printf("CloseFile RPC succeeded: %s\n", response.message().c_str());
        return 0;
    } else {
        fprintf(stderr, "CreateFile RPC failed: %s\n", status.error_message().c_str());
        return -1;
    }
}

std::pair<std::vector<struct Chunk>, std::string> metaserver_api_write(int fd, const void *buf, size_t num_bytes, off_t offset, int client_id) {
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
    request.set_client_id(client_id);

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

std::pair<std::vector<struct Chunk>, std::string> metaserver_api_read(int fd, const void *buf, size_t num_bytes, off_t offset, int client_id) {
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
    request.set_client_id(client_id);

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
        return {instructions, response.filename()};
    } else {
        fprintf(stderr, "WriteToFile RPC failed: %s\n", status.error_message().c_str());
    }
    return {{}, "FAIL"};
}

int metaserver_api_fstat(int fd, struct pfs_metadata *meta_data, int client_id) {
    printf("%s: called to fetch metadata from file.\n", __func__);

    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return -1;
    }

    pfsmeta::FileMetadataRequest request; pfsmeta::FileMetadataResponse response;
    request.set_file_descriptor(fd);
    request.set_client_id(client_id);

    grpc::ClientContext context;

    grpc::Status status = stub->FileMetadata(&context, request, &response);
    if (status.ok()) {
        printf("File Metadata RPC succeeded: %s\n", response.message().c_str());

        const pfsmeta::PFSMetadata& received_meta_data = response.meta_data();

        if (!meta_data) meta_data = new pfs_metadata;
        strncpy(meta_data->filename, received_meta_data.filename().c_str(), sizeof(meta_data->filename) - 1);
        meta_data->filename[sizeof(meta_data->filename) - 1] = '\0'; // Ensure null-termination
        meta_data->file_size = received_meta_data.file_size();
        meta_data->ctime = static_cast<time_t>(received_meta_data.ctime());
        meta_data->mtime = static_cast<time_t>(received_meta_data.mtime());

        const pfsmeta::PFSFileRecipe& recipe = received_meta_data.recipe();
        meta_data->recipe.stripe_width = recipe.stripe_width();

        // Populate chunks
        meta_data->recipe.chunks.clear(); // Clear any existing chunks in case of re-population
        for (const pfsmeta::ProtoChunk& proto_chunk : recipe.chunks()) {
            Chunk chunk;
            chunk.chunk_number = proto_chunk.chunk_number();
            chunk.server_number = proto_chunk.server_number();
            chunk.start_byte = proto_chunk.start_byte();
            chunk.end_byte = proto_chunk.end_byte();

            // Add the chunk to the recipe's chunks vector
            meta_data->recipe.chunks.push_back(chunk);
        }
        return 0;
    } else {
        fprintf(stderr, "File Metadata RPC failed: %s\n", status.error_message().c_str());
        return -1;
    }
}

int metaserver_api_delete(const char *filename, int client_id) {
    printf("%s: called to delete file.\n", __func__);

    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return -1;
    }

    pfsmeta::DeleteFileRequest request; pfsmeta::DeleteFileResponse response;
    request.set_filename(filename);
    request.set_client_id(client_id);

    grpc::ClientContext context;

    grpc::Status status = stub->DeleteFile(&context, request, &response);
    if (status.ok()) {
        printf("Delete File RPC succeeded: %s\n", response.message().c_str());
        return 0;
    } else {
        fprintf(stderr, "File Metadata RPC failed: %s\n", status.error_message().c_str());
        return -1;
    }
}

void metaserver_api_request_token(int fd, int start_byte, int end_byte, int type, int client_id) {
    printf("%s: called to request token.\n", __func__);

    auto stub = connect_to_metaserver();
    if (!stub) {
        std::cout << "Failed to connect to metaserver" << std::endl;
        return;
    }
    pfsmeta::TokenRequest request;
    request.set_file_descriptor(fd);
    request.set_start_byte(start_byte);
    request.set_end_byte(end_byte);
    request.set_type(type);
    request.set_client_id(client_id);

    stream->Write(request);

    auto& file_sync = file_sync_map[{descriptor_to_filename[fd], type}];
    std::unique_lock<std::mutex> lock(file_sync.mtx);
    std::cout << "I have sent the request, now I'll block myself until token arrives" << std::endl;
    file_sync.cv.wait(lock, [&file_sync] { return file_sync.token_ready; });  // Wait until token is ready
}

bool metaserver_api_check_tokens(int fd, int start_byte, int end_byte, int type, int client_id) {
    printf("%s: called to check token.\n", __func__);
    if (descriptor_to_filename.find(fd) == descriptor_to_filename.end()) {
        std::cerr << "Something went wrong!" << std::endl;
        return false;
    }
    std::string filename = descriptor_to_filename[fd];

    if (my_tokens.find(filename) == my_tokens.end()) return false;

    int current_start = start_byte;
    std::cout << "Checking my tokens for " << filename << std::endl;
    for (const auto& token : my_tokens[filename]) {
        std::cout << "Trying to cover with token " << token.to_string() << std::endl;
        int token_start = token.start_byte;
        int token_end = token.end_byte;
        int token_type = token.type;

        // Check if the token overlaps with the current uncovered range
        if (token_start <= current_start && token_end >= current_start) {
            // If type is read (1), any token can contribute
            if (type == 1 && (token_type == 1 || token_type == 2)) {
                current_start = token_end + 1;
            }
            // If type is write/read (2), only write/read tokens can contribute
            else if (type == 2 && token_type == 2) {
                current_start = token_end + 1;
            }
        }
        // If the entire range is covered, return true
        if (current_start >= end_byte) {
            return true;
        }
    }
    // If the range is not fully covered, return false
    return false;
}





