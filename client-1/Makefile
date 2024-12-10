.SUFFIXES:
.PHONY: default clean
default: client-1-1 client-1-2 client-1-3 client-1-1x

OBJS = ../pfs_common/pfs_common.o \
		../pfs_proto/pfs_fileserver.pb.o ../pfs_proto/pfs_fileserver.grpc.pb.o \
		../pfs_proto/pfs_metaserver.pb.o ../pfs_proto/pfs_metaserver.grpc.pb.o \
		../pfs_client/pfs_api.o ../pfs_client/pfs_cache.o \
		../pfs_metaserver/pfs_metaserver_api.o ../pfs_fileserver/pfs_fileserver_api.o

%: %.o $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(LDLIBS)

%.o: %.cpp ../pfs_common/pfs_config.hpp
	$(CXX) $(CXXFLAGS) -o $@ -c $< $(LDLIBS)

clean:
	rm -f client-1-1 client-1-2 client-1-3 output.txt
