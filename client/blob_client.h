#ifndef BLOB_CLIENT_H
#define BLOB_CLIENT_H

#include <memory>
#include <unordered_map>
#include "protos/blobstore.grpc.pb.h"

using blobstore::BlobStore;

class BlobClient {
private:
  std::string server1_address_;
  std::string server2_address_;
  std::string primary_address_;
  std::unique_ptr<BlobStore::Stub> clientStub_;
  int retry_count_;
  int max_retry_count_;
  std::string log_prefix_;

public:
  BlobClient(const std::string &server1_address, const std::string &server2_address, const int max_retry_count);
  void changePrimary();
  void connect();
  int read(int64_t address, std::string &data);
  int write(int64_t address, std::string data);
};

#endif

