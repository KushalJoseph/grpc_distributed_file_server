# Distributed Parallel File Server using gRPC and Protocol Buffers

This repository contains the code for a Distributed Parallel File Server. Clients interact with files, which will be **striped across multiple file servers**. Clients will interact directly with the metadata server and file servers to read from and write to files. 

The communication between the Client API, and the Metaserver, and Fileservers is handled using **gRPC and Protocol Buffers**. I have defined several RPC (remote procedure call) definitions in the .proto files, one each for the Metaserver, and Fileservers. Each rpc call is associated with a "message", a collection of data which contains each rpc's request parameters and response fields.

Further, the file system is sequentially consistent, allowing concurrency between clients. I have used a token management mechanism, with tokens handed out by the metadata server to achieve this goal. A Read (or Write) token/lock needs to be first obtained by any client, for the range of concerned bytes, before it performs the Read (or Write). Any conflicts (Reads and Writes, or multiple Writes) coming to intersecting ranges will be serialized by the Metadata server. At the same time, the concurrency for non-conflicting operations is maximized, e.g., client0 may write to abc.txt from byte 300 to 500. Client1 may concurrently write to abc.txt from byte 800 to 1100. Client2 may concurrently write to abc.txt from byte 200 to 250.

The system has 3 components:
- A metadata server
- File server(s), client files will be striped across these file servers
- Client(s) with client cache

You can use the client API to create your own clients which interact with the File Server. The API is defined below:

```int pfs_initialize()```
Init the PFS client. Returns a positive value client id allocated by the metadata server. Returns -1 on error (e.g., can't communicate with metadata server, file servers (total of NUM_FILE_SERVERS) are not online yet, etc.)

```int pfs_create(const char *filename, int stripe_width);```
Creates a file with the name filename. The file will be in striped into stripe_width number of servers. Return 0 on success. Returns -1 on error (e.g., duplicate filename, stripe_width larger than NUM_FILE_SERVERS, etc). This will also create the metadata on the metadata server. Users can access the metadata via pfs_fstat().

```int pfs_open(const char *filename, int mode);```
Opens a file with name filename. The mode will be either 1 (read) or 2 (read/write). Return a positive value file descriptor. Returns -1 on error (e.g., non-existing file, duplicate open, etc).

```int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset);```
Reads num_bytes bytes from the file fd, reading starts from offset~. Return the actual number of read bytes (may be smaller than the requested, if the offset is close to end-of-file). (A return of zero indicates end-of-file.) Return -1 on error (e.g., offset is larger than the filesize, non-existing file descriptor, communication with metadata/file server failed, etc.)

```int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset);```
Writes num_bytes bytes to the file fd, writing starts from offset~. Return the number of written bytes. Offset will be 0<=offset<=filesize. Return -1 on error (e.g., file open mode is not read/write, non-existing file descriptor, offset is larger than the filesize, etc.).

```int pfs_close(int fd);```
Closes a file with file descriptor fd. Return 0 on success. Returns -1 on error (e.g., non-existing file descriptor, etc.).

```int pfs_delete(const char *filename);```
Deletes a file with the name filename. Return 0 on success. Returns -1 on error.

```int pfs_fstat(int fd, struct pfs_metadata *meta_data);```
Writes the metadata of file fd to the pointer meta_data. This API will fetch the most recent data on the metadata server. Returns 0 on success. Returns -1 on error.
