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
    std::unordered_map<std::string, struct pfs_metadata> files;             // filename: Metadata

    std::unordered_map<int, std::pair<std::string, int>> descriptor; // descriptor : <filename, mode>
    std::unordered_map<std::string, int> fileNameToDescriptor;       // filename: descriptor

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

        if (files.find(filename) != files.end()) {
            std::string msg = "Cannot create file. File already exists!";
            std::cerr << msg << std::endl;
            reply->set_message(msg);
            reply->set_status_code(1);
            return Status(grpc::StatusCode::INVALID_ARGUMENT, msg);
        }

        struct pfs_metadata file_metadata;
        struct pfs_filerecipe recipe;
        recipe.stripe_width = stripe_width;
        recipe.chunks = std::vector<struct Chunk> (); // initialize empty chunks vector      

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
            if(descriptor[fd].second == MODE_WRITE) {
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

            reply->set_message("File opened for you to read.");
            reply->set_file_descriptor(fd);
            printf("%s: Received Open RPC call to read.\n", __func__);

            descriptor[fd] = {filename, MODE_READ};
            fileNameToDescriptor[filename] = fd;
            return Status::OK;
        } else if (mode == MODE_WRITE) {
            int fd = next_fd++;
            used_fds.insert(fd);

            reply->set_message("File opened for you to write.");
            reply->set_file_descriptor(fd);
            printf("%s: Received Open RPC call to write.\n", __func__);

            descriptor[fd] = {filename, MODE_WRITE};
            fileNameToDescriptor[filename] = fd;
            return Status::OK;
        }
    }

    bool checkIfFileIsOpen(int fd) {
        return descriptor.find(fd) != descriptor.end();
    }

    Status CloseFile(ServerContext* context, const pfsmeta::CloseFileRequest* request, pfsmeta::CloseFileResponse* reply) override {        
        int fd = request->file_descriptor();
        if (checkIfFileIsOpen(fd) == false) {
            std::string msg = "File may already be closed!";
            std::cerr << msg << std::endl;
            reply->set_message(msg);
            reply->set_status_code(0);
            return Status::OK;
        }
        std::string filename = descriptor[fd].first;

        // remove all records of the file being open, i.e, erase from the maps.
        fileNameToDescriptor.erase(filename);
        descriptor.erase(fd);

        std::string replyMsg = "File " + filename + " closed successfully.\n";
        reply->set_message(replyMsg);
        reply->set_status_code(0);  
        return Status::OK;
    }


    Status WriteToFile(ServerContext* context, const pfsmeta::WriteToFileRequest* request, pfsmeta::WriteToFileResponse* reply) override {        
        int fd = request->file_descriptor();
        std::string buf = request->buf();
        int num_bytes = request->num_bytes();
        int offset = request->offset();

        std::cout << "\nrequested to write: " << num_bytes << " from " << offset << std::endl << std::endl;
        
        if (checkIfFileIsOpen(fd) == false) {
            std::string msg = "File is not open!";
            std::cerr << msg << std::endl;
            reply->set_message(msg);
            return Status(grpc::StatusCode::INVALID_ARGUMENT, msg);
        }

        std::string filename = descriptor[fd].first;
        if (files.find(filename) == files.end()) {
            std::string msg = "File does not exist or was already deleted!";
            std::cerr << msg << std::endl;
            reply->set_message(msg);
            return Status(grpc::StatusCode::INVALID_ARGUMENT, msg);
        }

        struct pfs_metadata &file_metadata = files[filename];
        int cur_file_size = file_metadata.file_size;
        if (offset > cur_file_size) {
            std::string msg = "Requested Offset " + std::to_string(offset) + ", cannot be greater than current file size, " + std::to_string(cur_file_size);
            std::cerr << msg << std::endl;
            reply->set_message(msg);
            return Status(grpc::StatusCode::INVALID_ARGUMENT, msg);
        }

        std::vector<struct Chunk> &chunks = file_metadata.recipe.chunks;
        int start_chunk = offset / 1024; 
        int end_chunk = (offset + num_bytes - 1) / 1024;
        int bytes_written = 0;

        std::vector<struct Chunk> write_instructions;
        for (int chunk_number = start_chunk; chunk_number <= end_chunk; chunk_number++) {
            int start_byte_for_instruction = std::max(chunk_number * 1024, offset); // start from the beginning of the chunk, or from the offset.
            int end_byte_for_instruction = std::min((chunk_number + 1) * 1024 - 1, offset + num_bytes - 1); // end of the chunk, or offset + bytes, MINIMUM
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

        std::string replyMsg = "Done";
        reply->set_message(replyMsg);
        reply->set_filename(filename);
        for (const struct Chunk &instr: write_instructions) {
            WriteInstruction* write_instruction = reply->add_instructions();
            write_instruction->set_chunk_number(instr.chunk_number);
            write_instruction->set_server_number(instr.server_number);
            write_instruction->set_start_byte(instr.start_byte);
            write_instruction->set_end_byte(instr.end_byte);
        }
        return Status::OK;
    }

    Status ReadFile(ServerContext* context, const pfsmeta::ReadFileRequest* request, pfsmeta::ReadFileResponse* reply) override {        
        int fd = request->file_descriptor();
        std::string buf = request->buf();
        int num_bytes = request->num_bytes();
        int offset = request->offset();

        std::cout << "\nrequested to read: " << num_bytes << " from " << offset << std::endl << std::endl;
        
        if (checkIfFileIsOpen(fd) == false) {
            std::string msg = "File is not open!";
            std::cerr << msg << std::endl;
            reply->set_message(msg);
            return Status(grpc::StatusCode::INVALID_ARGUMENT, msg);
        }

        std::string filename = descriptor[fd].first;
        if (files.find(filename) == files.end()) {
            std::string msg = "File does not exist or was already deleted!";
            std::cerr << msg << std::endl;
            reply->set_message(msg);
            return Status(grpc::StatusCode::INVALID_ARGUMENT, msg);
        }

        struct pfs_metadata &file_metadata = files[filename];
        std::vector<struct Chunk> &chunks = file_metadata.recipe.chunks;
        int start_chunk = offset / 1024; 
        int end_chunk = (offset + num_bytes - 1) / 1024;
        int bytes_read = 0;

        std::vector<struct Chunk> read_instructions;
        for (int chunk_number = start_chunk; chunk_number <= end_chunk; chunk_number++) {
            int start_byte_for_instruction = std::max(chunk_number * 1024, offset); // start from the beginning of the chunk, or from the offset.
            int end_byte_for_instruction = std::min((chunk_number + 1) * 1024 - 1, std::min((int) file_metadata.file_size - 1, offset + num_bytes - 1)); // end of the chunk, or end of file or, offset + bytes, MINIMUM
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

        std::string replyMsg = "Done";
        reply->set_message(replyMsg);
        reply->set_filename(filename); 
        for (const struct Chunk &instr: read_instructions) {
            ReadInstruction* read_instruction = reply->add_instructions();
            read_instruction->set_chunk_number(instr.chunk_number);
            read_instruction->set_server_number(instr.server_number);
            read_instruction->set_start_byte(instr.start_byte);
            read_instruction->set_end_byte(instr.end_byte);
        }
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
