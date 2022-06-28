#include "blob_client.h"
#include <grpcpp/grpcpp.h>
#include <memory>
#include <fstream>

#include "absl/status/status.h"
#include "protos/blobstore.grpc.pb.h"
#include "resources/utils.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using blobstore::BlobStore;

BlobClient::BlobClient(const std::string &server1_address, const std::string &server2_address, const int max_retry_count)
    : server1_address_(server1_address), server2_address_(server2_address), max_retry_count_(max_retry_count) {
  retry_count_ = 0;
  // Default primary server is the first server
  primary_address_ = server1_address_;
  log_prefix_ = "\t[ClientLib]: ";
}

void BlobClient::changePrimary(){
  if(primary_address_ == server1_address_){
    primary_address_ = server2_address_;
  } else {
    primary_address_ = server1_address_;
  }
}

// Connect to server
void BlobClient::connect() {
  fprintf(stderr, "%s Connecting to server %s\n", log_prefix_.c_str(), primary_address_.c_str());
  clientStub_ = BlobStore::NewStub(grpc::CreateChannel(
      primary_address_, grpc::InsecureChannelCredentials()));
}

// Read from server
int BlobClient::read(int64_t address, std::string &data) {
  if (!clientStub_) {
    fprintf(stderr, "%s Client not connected.\n", log_prefix_.c_str());
    return -1;
  }

  ClientContext context;
  blobstore::ReadRequest request;
  request.set_address(address);

  blobstore::ReadResponse response;
  Status status = clientStub_->Read(&context, request, &response);
  if (status.ok()) {
    // reset retry_count_ on success
    retry_count_ = 0;

    if(response.status() == "FAILURE") {
      primary_address_ = response.primary_ip();
      connect();
      return read(address, data);
    } else if (response.status() == "UNAVAILABLE") {
      return read(address, data);
    } else {
      data = response.data();
      return data.size();
    }
  } else {
    fprintf(stderr, "%s %d: %s\n", log_prefix_.c_str(), status.error_code(), status.error_message().c_str());
    fprintf(stderr, "%s Failed to read from server. Retrying ...\n", log_prefix_.c_str());
    
    retry_count_++;
    if (retry_count_ >= max_retry_count_) {
      // Error on reaching max retry count and reset retry_count_
      retry_count_ = 0;
      return -1;
    }
    
    // Change primary server and retry for each failure
    changePrimary();
    connect();

    // If this read request had a crash address, retry with correct address
    if(Utils::get_crash_type(address) != CrashType::NO_CRASH) {
      address = Utils::get_address(address);
    }
    
    return read(address, data); // can cause stack overflow if retry_count_ is too high
  }
}

// Write to server
int BlobClient::write(int64_t address, std::string data) {
  if (!clientStub_) {
    fprintf(stderr, "%s Client not connected.\n", log_prefix_.c_str());
    return -1;
  }

  ClientContext context;
  blobstore::WriteRequest request;
  request.set_address(address);
  request.set_data(data);

  blobstore::WriteResponse response;
  Status status = clientStub_->Write(&context, request, &response);
  if (status.ok()) {
    // reset retry_count_ on success
    retry_count_ = 0;

    if(response.status() == "FAILURE") {
      primary_address_ = response.primary_ip();
      connect();
      return write(address, data);
    } else if (response.status() == "UNAVAILABLE") {
      // TODO: if write unavailable, exit
      return -1;
    } else {
      return data.length();
    }
  } else {
    fprintf(stderr, "%s %d: %s\n", log_prefix_.c_str(), status.error_code(), status.error_message().c_str());
    fprintf(stderr, "%s Failed to write to server. Retrying ...\n", log_prefix_.c_str());

    retry_count_++;
    if (retry_count_ >= max_retry_count_) {
      // Error on reaching max retry count and reset retry_count_
      retry_count_ = 0;
      return -1;
    }

    // Change primary server and retry for each failure
    changePrimary();
    connect();

    // If this write request had a crash address, retry with correct address
    if(Utils::get_crash_type(address) != CrashType::NO_CRASH) {
      address = Utils::get_address(address);
    }

    return write(address, data);
  }
}

