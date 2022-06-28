#ifndef BLOB_SERVER_H_
#define BLOB_SERVER_H_
#include <string>
#include "absl/status/status.h"
#include <grpcpp/grpcpp.h>
#include "logger.h"
#include <shared_mutex>
#include <thread>

#ifdef BAZEL_BUILD
#else
#include "protos/blobstore.grpc.pb.h"
#endif

#define NUM_MUTEXES 32

enum BlobServerState {
  PRIMARY,
  BACKUP,
};

class StoreInternalClient {
  public:
    StoreInternalClient(std::shared_ptr<grpc::Channel> channel)
        : stub_(blobstore::StoreInternal::NewStub(channel)) {}

    grpc::Status Ping(const blobstore::PingRequest& request, blobstore::PingResponse* response) {
      grpc::ClientContext context;
      return stub_->Ping(&context, request, response);
    }
  
    grpc::Status Prepare(const blobstore::PrepareRequest& request, blobstore::PrepareResponse* response) {
      grpc::ClientContext context;
      return stub_->Prepare(&context, request, response);
    }
  
    grpc::Status Commit(const blobstore::CommitRequest& request, blobstore::CommitResponse* response) {
      grpc::ClientContext context;
      return stub_->Commit(&context, request, response);
    }
  
    grpc::Status Recovery(const blobstore::RecoveryRequest& request, blobstore::RecoveryResponse* response) {
      grpc::ClientContext context;
      // Receive recovery records from primary
      // Replace log entries
      // For each record: Create tmp file and rename to actual file
      return stub_->Recovery(&context, request, response);
    }
  
  private:
    std::unique_ptr<blobstore::StoreInternal::Stub> stub_;
};

class BlobServer {
  public:
  std::string get_other_ip(){
    return other_ip_;
  }
  BlobServerState get_state(){
    return state;
  }
  void set_state(BlobServerState state){
    this->state = state;
  }

  std::shared_timed_mutex& getMutex() {
    return recovery_mutex_;
  }

  void setBackupAlive(bool alive) {
    backupAlive = alive;
  }

  BlobServer() = delete;
  explicit BlobServer(std::string root_path, 
                      std::string self_ip, 
                      std::string other_ip);
    
  absl::Status Read(int64_t address, std::string* data);
  absl::Status Write(int64_t address, const std::string& data);
  int PrepareLocal(int64_t address, const std::string& data);
  int CommitLocal(int64_t txId, int64_t address);
  std::vector<blobstore::LogEntry>  MergeAndRefreshLogsLocal(std::vector<blobstore::LogEntry>& backup_logs);
  std::vector<blobstore::RecoveryRecord> CreateRecoveryResponse(std::vector<blobstore::LogEntry>& fresh_logs);
  void ServerInit();
  private:
  void ConnectToOtherBlobServer();
  bool CheckPrimaryFailure();
  absl::Status Recovery();
  int ReplayRecoveryRecords(blobstore::RecoveryResponse& recovery_response);
  absl::Status GetLogEntries(std::vector<blobstore::LogEntry>& entries);
  std::string GetFilePath(std::string root, int64_t address);
  int Prepare(int64_t address, const std::string& data);
  int Commit(int64_t address);
  int64_t generate_txId();
  
  private:
  BlobServerState state;
  bool backupAlive;
  std::string root_path_;
  std::string tmp_path_;
  std::string self_ip_;
  std::string other_ip_;
  std::unique_ptr<StoreInternalClient> store_internal_client_;
  std::shared_ptr<Logger> logger_;
  std::shared_timed_mutex mutex_;
  std::array<std::mutex, NUM_MUTEXES> mutex_pool_;
  std::shared_timed_mutex recovery_mutex_;
};

#endif // BLOB_SERVER_H_