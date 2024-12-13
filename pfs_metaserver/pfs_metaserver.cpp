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
using grpc::ServerReaderWriter;
using grpc::Status;
using namespace pfsmeta;

const int MODE_READ = 1;
const int MODE_WRITE = 2;

class PFSMetadataServerImpl final : public PFSMetadataServer::Service {
private:
    std::unordered_map<std::string, struct pfs_metadata> files;                     // filename: Metadata
    std::unordered_map<int, std::pair<std::string, int>> descriptor;                // descriptor : <filename, mode>
    std::unordered_map<std::string, int> fileNameToDescriptor;                      // filename: descriptor

    // filename: [{token - connection to client who owns it}]
    std::map<std::string, std::map<FileToken, ServerReaderWriter<pfsmeta::ServerNotification, pfsmeta::TokenRequest>*>> file_tokens_;
    std::set<ServerReaderWriter<pfsmeta::ServerNotification, pfsmeta::TokenRequest>*> client_streams_;
    std::mutex mutex_;

    int next_fd = 3;
    int next_client_id = 1;
    std::set<int> used_fds;

    template <typename ReplyType>
    grpc::Status error(const std::string& msg, ReplyType* reply) {
        std::cerr << msg << std::endl;
        reply->set_message(msg);
        reply->set_status_code(1);
        return Status(grpc::StatusCode::INVALID_ARGUMENT, msg);
    }

    template <typename ReplyType>
    grpc::Status success(const std::string& msg, ReplyType* reply) {
        reply->set_message(msg);
        reply->set_status_code(0);  
        return Status::OK;
    }

public:
    Status Ping(ServerContext* context, const PingRequest* request, PingResponse* reply) override {
        printf("%s: Received ping RPC call.\n", __func__);
        reply->set_message("Thanks for the Ping. I, the metaserver am alive!");
        return Status::OK;
    }

    Status Initialize(ServerContext* context, const InitRequest* request, InitResponse* reply) override {
        printf("%s: Received Initialize RPC call.\n", __func__);
        reply->set_message("Initialize successful!");
        reply->set_client_id(next_client_id);
        next_client_id++;
        return Status::OK;
    }

    Status CreateFile(ServerContext* context, const pfsmeta::CreateFileRequest* request, pfsmeta::CreateFileResponse* reply) override {        
        std::string filename = request->filename();
        int stripe_width = request->stripe_width();
        int client_id = request->client_id();

        if(stripe_width > NUM_FILE_SERVERS) return error("Stripe width cannot exceed the number of file servers!", reply);
        
        if (files.find(filename) != files.end()) return error("Cannot create file. File already exists!", reply);
            
        struct pfs_metadata file_metadata;
        struct pfs_filerecipe recipe;
        recipe.stripe_width = stripe_width;
        recipe.chunks = std::vector<struct Chunk> (); // initialize empty chunks vector      

        std::strncpy(file_metadata.filename, filename.c_str(), sizeof(file_metadata.filename) - 1);
        file_metadata.filename[sizeof(file_metadata.filename) - 1] = '\0'; // Ensure null termination
        file_metadata.file_size = 0;
        file_metadata.recipe = recipe;
        file_metadata.ctime = std::time(nullptr);
        file_metadata.mtime = 0;

        // creation, updation time
        files[filename] = file_metadata;
        return success("File " + filename + " created successfully.\n", reply);
    }

    Status OpenFile(ServerContext* context, const OpenFileRequest* request, OpenFileResponse* reply) override {
        printf("%s: Received Open RPC call to read.\n", __func__);
        std::string filename = request->filename();
        if(files.find(filename) == files.end()) return error("File does not exist!", reply);
        
        int client_id = request->client_id();

        int mode = request->mode();
        if (mode == MODE_READ) {
            int fd = next_fd++;
            used_fds.insert(fd);
            descriptor[fd] = {filename, MODE_READ};
            fileNameToDescriptor[filename] = fd;
            reply->set_file_descriptor(fd);
            return success("File opened for you to read.", reply);
        } else if (mode == MODE_WRITE) {
            int fd = next_fd++;
            used_fds.insert(fd);
            descriptor[fd] = {filename, MODE_WRITE};
            fileNameToDescriptor[filename] = fd;
            reply->set_file_descriptor(fd);
            return success("File opened for you to write.", reply);
        } else {
            return error("Wrong Mode", reply);
        }
    }

    Status FileMetadata(ServerContext* context, const FileMetadataRequest* request, FileMetadataResponse* reply) override {
        printf("%s: Received File Metadata RPC call to read.\n", __func__);
        int fd = request->file_descriptor();
        if (descriptor.find(fd) == descriptor.end()) return error("File is not open", reply);
        std::string filename = descriptor[fd].first;
        if (files.find(filename) == files.end()) return error("Something went wrong, couldn't find file", reply);
        
        int client_id = request->client_id();

        struct pfs_metadata meta_data = files[filename];
        PFSMetadata* pfs_meta = reply->mutable_meta_data();
        pfs_meta->set_filename(meta_data.filename);
        pfs_meta->set_file_size(meta_data.file_size);
        pfs_meta->set_ctime(meta_data.ctime);
        pfs_meta->set_mtime(meta_data.mtime);
        PFSFileRecipe* file_recipe = pfs_meta->mutable_recipe();
        file_recipe->set_stripe_width(meta_data.recipe.stripe_width);

        // Convert chunks from pfs_filerecipe to ProtoChunk
        for (const Chunk& chunk : meta_data.recipe.chunks) {
            ProtoChunk* proto_chunk = file_recipe->add_chunks();
            proto_chunk->set_chunk_number(chunk.chunk_number);
            proto_chunk->set_server_number(chunk.server_number);
            proto_chunk->set_start_byte(chunk.start_byte);
            proto_chunk->set_end_byte(chunk.end_byte);
        }

        return success("Sent File Metadata", reply);
    }

    Status DeleteFile(ServerContext* context, const DeleteFileRequest* request, DeleteFileResponse* reply) override {
        printf("%s: Received Delete File RPC call to read.\n", __func__);
        std::string filename = request->filename();
        if (files.find(filename) == files.end()) return error("Something went wrong, couldn't find file", reply);
        
        int client_id = request->client_id();

        if (fileNameToDescriptor.find(filename) != fileNameToDescriptor.end()) return error("File is still open. Cannot Delete", reply);
        
        // remove file, fds from maps
        fileNameToDescriptor.erase(filename);
        files.erase(filename);

        return success("File records Deleted from Metaserver", reply);
    }

    Status CloseFile(ServerContext* context, const pfsmeta::CloseFileRequest* request, pfsmeta::CloseFileResponse* reply) override {        
        printf("%s: Received File Metadata RPC call to close file.\n", __func__);
        int fd = request->file_descriptor();
        if (descriptor.find(fd) == descriptor.end()) return success("File may already be closed!", reply);
        std::string filename = descriptor[fd].first;

        files[filename].mtime = std::time(nullptr);
        // remove all records of the file being open, i.e, erase from the maps.
        fileNameToDescriptor.erase(filename);
        descriptor.erase(fd);

        // delete tokens held by client, since they are closing the file now
        auto& ranges = file_tokens_[filename]; // map<FileToken, stream>
        for (auto it = ranges.begin(); it != ranges.end(); ) {
            struct FileToken existing_token = it->first;
            auto* conflicting_stream = it->second;
            if (existing_token.client_id == request->client_id()) {
                std::cout << "Deleting this token from my record: " << existing_token.to_string();
                it = ranges.erase(it);
            } else {
                ++it;
            }
        }
        return success("File " + filename + " closed successfully.\n", reply);
    }


    Status WriteToFile(ServerContext* context, const pfsmeta::WriteToFileRequest* request, pfsmeta::WriteToFileResponse* reply) override {        
        int fd = request->file_descriptor();
        std::string buf = request->buf();
        int num_bytes = request->num_bytes();
        int offset = request->offset();
        int client_id = request->client_id();

        std::cout << "\nClient " << client_id << " requested to write: " << num_bytes << " from " << offset << std::endl << std::endl;
        
        if (descriptor.find(fd) == descriptor.end()) return error("File doesn't exist or is not open!", reply);

        std::string filename = descriptor[fd].first;
        if (files.find(filename) == files.end()) return error("File does not exist or was already deleted!", reply);

        struct pfs_metadata &file_metadata = files[filename];
        int cur_file_size = file_metadata.file_size;
        if (offset > cur_file_size) return error("Requested Offset " + std::to_string(offset) + ", cannot be greater than current file size, " + std::to_string(cur_file_size), reply);

        std::vector<struct Chunk> &chunks = file_metadata.recipe.chunks;
        int start_chunk = offset / (PFS_BLOCK_SIZE * STRIPE_BLOCKS); 
        int end_chunk = (offset + num_bytes - 1) / (PFS_BLOCK_SIZE * STRIPE_BLOCKS);
        int bytes_written = 0;

        std::vector<struct Chunk> write_instructions;
        for (int chunk_number = start_chunk; chunk_number <= end_chunk; chunk_number++) {
            int start_byte_for_instruction = std::max(chunk_number * (PFS_BLOCK_SIZE * STRIPE_BLOCKS), offset); // start from the beginning of the chunk, or from the offset.
            int end_byte_for_instruction = std::min((chunk_number + 1) * (PFS_BLOCK_SIZE * STRIPE_BLOCKS) - 1, offset + num_bytes - 1); // end of the chunk, or offset + bytes, MINIMUM
            int server_number = chunk_number % file_metadata.recipe.stripe_width;

            // check if this chunk exists
            auto it = std::find_if(chunks.begin(), chunks.end(), [chunk_number](const Chunk& c) {
                return c.chunk_number == chunk_number;
            });
            
            struct Chunk chunk_for_instruction = Chunk{chunk_number, server_number, start_byte_for_instruction, end_byte_for_instruction};
            bytes_written += (end_byte_for_instruction - start_byte_for_instruction + 1);
            if (it == chunks.end()) {
                chunks.push_back(chunk_for_instruction); // it's a brand new chunk
                file_metadata.file_size += (end_byte_for_instruction - start_byte_for_instruction + 1);
            } else {
                struct Chunk& existing_chunk = *it;
                int current_chunk_size = existing_chunk.end_byte - existing_chunk.start_byte + 1;
                if (end_byte_for_instruction > existing_chunk.end_byte) {
                    existing_chunk.end_byte = end_byte_for_instruction;
                    int new_chunk_size = existing_chunk.end_byte - existing_chunk.start_byte + 1;
                    file_metadata.file_size += (new_chunk_size - current_chunk_size);
                }
            }
            write_instructions.push_back(chunk_for_instruction);
        }    

        std::cout << "\nWrite Confirmation: \n" << filename << "\n" << files[filename].to_string() << std::endl;
        reply->set_filename(filename);
        for (const struct Chunk &instr: write_instructions) {
            WriteInstruction* write_instruction = reply->add_instructions();
            write_instruction->set_chunk_number(instr.chunk_number);
            write_instruction->set_server_number(instr.server_number);
            write_instruction->set_start_byte(instr.start_byte);
            write_instruction->set_end_byte(instr.end_byte);
        }
        return success("Done", reply);
    }

    Status ReadFile(ServerContext* context, const pfsmeta::ReadFileRequest* request, pfsmeta::ReadFileResponse* reply) override {        
        int fd = request->file_descriptor();
        std::string buf = request->buf();
        int num_bytes = request->num_bytes();
        int offset = request->offset();
        int client_id = request->client_id();

        std::cout << "\nClient " << client_id << " requested to read: " << num_bytes << " from " << offset << std::endl << std::endl;
        
        if (descriptor.find(fd) == descriptor.end()) return error("File doesn't exist or is not open!", reply);

        std::string filename = descriptor[fd].first;
        if (files.find(filename) == files.end()) return error("File does not exist or was already deleted!", reply);

        struct pfs_metadata &file_metadata = files[filename];
        std::vector<struct Chunk> &chunks = file_metadata.recipe.chunks;
        int start_chunk = offset / (PFS_BLOCK_SIZE * STRIPE_BLOCKS); 
        int end_chunk = (offset + num_bytes - 1) / (PFS_BLOCK_SIZE * STRIPE_BLOCKS);
        int bytes_read = 0;

        std::vector<struct Chunk> read_instructions;
        for (int chunk_number = start_chunk; chunk_number <= end_chunk; chunk_number++) {
            int start_byte_for_instruction = std::max(chunk_number * (PFS_BLOCK_SIZE * STRIPE_BLOCKS), offset); // start from the beginning of the chunk, or from the offset.
            int end_byte_for_instruction = std::min((chunk_number + 1) * (PFS_BLOCK_SIZE * STRIPE_BLOCKS) - 1, std::min((int) file_metadata.file_size - 1, offset + num_bytes - 1)); // end of the chunk, or end of file or, offset + bytes, MINIMUM
            int server_number = chunk_number % file_metadata.recipe.stripe_width;

            // check if this chunk exists
            auto it = std::find_if(chunks.begin(), chunks.end(), [chunk_number](const Chunk& c) {
                return c.chunk_number == chunk_number;
            });
            
            struct Chunk chunk_for_instruction = Chunk{chunk_number, server_number, start_byte_for_instruction, end_byte_for_instruction};
            if (it == chunks.end()) {
                break;
            }
            bytes_read += (end_byte_for_instruction - start_byte_for_instruction + 1);
            read_instructions.push_back(chunk_for_instruction);
        }    

        reply->set_filename(filename); 
        for (const struct Chunk &instr: read_instructions) {
            ReadInstruction* read_instruction = reply->add_instructions();
            read_instruction->set_chunk_number(instr.chunk_number);
            read_instruction->set_server_number(instr.server_number);
            read_instruction->set_start_byte(instr.start_byte);
            read_instruction->set_end_byte(instr.end_byte);
        }
        return success("Done", reply);
    }

    Status TokenStream(ServerContext* context, ServerReaderWriter<ServerNotification, TokenRequest>* stream) override {
        std::string client_id;
        // Store the stream for this client
        {
            std::unique_lock<std::mutex> lock(mutex_);
            client_streams_.insert(stream);
        }

        // Process incoming token requests
        TokenRequest request;
        while (stream->Read(&request)) {
            int fd = request.file_descriptor();
            if (descriptor.find(fd) == descriptor.end()) return Status(grpc::StatusCode::INVALID_ARGUMENT, "Please open the file first\n");

            std::string filename = descriptor[fd].first;
            handleTokenRequest(filename, request, stream);
        }

        // WHEN CLIENT FINISHES Remove the stream when the client disconnects
        // {
        //     std::unique_lock<std::mutex> lock(mutex_);
        //     client_streams_.erase(stream);
        // }
        return Status::OK;
    }

    void handleTokenRequest(std::string filename, const TokenRequest& request, ServerReaderWriter<ServerNotification, TokenRequest>* stream) {
        int start_byte = request.start_byte();
        int end_byte = request.end_byte();
        int type = request.type();
        int client_id = request.client_id();
        
        std::cout << "Receieved token request from " << client_id << " for " << filename << (type == 1 ? " read " : " write ") << start_byte << "-" << end_byte << std::endl;

        FileToken requested_range{start_byte, end_byte, type, client_id};

        std::unique_lock<std::mutex> lock(mutex_);

        auto& ranges = file_tokens_[filename]; // map<FileToken, stream>
        for (auto it = ranges.begin(); it != ranges.end(); ) {
            struct FileToken existing_token = it->first;
            auto* conflicting_stream = it->second;

            std::cout << "Checking Ranges for Overlap: " << existing_token.start_byte << "-" << existing_token.end_byte << " VS " << start_byte << "-" << end_byte << std::endl;
            if (existing_token.client_id != client_id && existing_token.overlaps(requested_range)) {
                std::cout << "They Overlap" << std::endl;
                std::vector<FileToken> new_ranges = existing_token.subtract(requested_range);

                std::cout << "Sending revocation to client " << existing_token.client_id << std::endl;
                ServerNotification notification;
                auto* revocation = notification.mutable_revocation();
                revocation->set_filename(filename);

                auto* new_token = revocation->add_new_tokens();
                new_token->set_start_byte(existing_token.start_byte);
                new_token->set_end_byte(existing_token.end_byte);
                new_token->set_type(existing_token.type);
                new_token->set_client_id(existing_token.client_id);

                for (const auto& range : new_ranges) {
                    auto* new_token = revocation->add_new_tokens();
                    new_token->set_start_byte(range.start_byte);
                    new_token->set_end_byte(range.end_byte);
                    new_token->set_type(range.type);
                    new_token->set_client_id(range.client_id);
                }
                conflicting_stream->Write(notification);
                it = ranges.erase(it);
            } else {
                std::cout << "They don't overlap" << std::endl;
                ++it;
            }
        }
        ranges[requested_range] = stream;
        sendGrant(stream, filename, requested_range, client_id, type);
    }

    void sendGrant(ServerReaderWriter<ServerNotification, TokenRequest>* stream,
                   const std::string& filename, const FileToken& range, int client_id, int type) {
        std::cout << "Granting to this client, i.e, " << client_id << std::endl;
        ServerNotification notification;
        auto* grant = notification.mutable_grant();
        grant->set_start_byte(range.start_byte);
        grant->set_end_byte(range.end_byte);
        grant->set_client_id(client_id);
        grant->set_type(type);
        grant->set_filename(filename);
        stream->Write(notification);
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
