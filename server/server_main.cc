#include "blob_server.h"
#include "protos/blobstore.grpc.pb.h"
#include <cstdio>
#include <string>
#include <mutex>
#include <thread>
#include <pthread.h>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include "absl/strings/str_cat.h"
#include "absl/status/status.h"
#include "absl/strings/str_join.h"
#ifdef BAZEL_BUILD
// #include "examples/protos/helloworld.grpc.pb.h"
#else
#include "protos/blobstore.grpc.pb.h"
#endif

// #define performance_measure

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
// using grpc::grpc::Status;
using blobstore::BlobStore;
using blobstore::ReadRequest;
using blobstore::ReadResponse;
using blobstore::WriteRequest;
using blobstore::WriteResponse;
using blobstore::StoreInternal;
using blobstore::PingRequest;
using blobstore::PingResponse;
using blobstore::PrepareRequest;
using blobstore::PrepareResponse;
using blobstore::CommitRequest;
using blobstore::CommitResponse;
using blobstore::RecoveryRequest;
using blobstore::RecoveryResponse;
using blobstore::RecoveryRecord;
using blobstore::LogEntry;

class BlobStoreImpl final : public BlobStore::Service {
  private: 
  std::shared_ptr<BlobServer>  blobserver_;
  grpc::Status handleStatusCode(absl::Status status){
    if (status != absl::OkStatus()) {
        if(status.code() == absl::StatusCode::kNotFound) {
          return grpc::Status(grpc::StatusCode::NOT_FOUND, "Please contact other server");
        } else {
            return grpc::Status(grpc::StatusCode::INTERNAL, "Internal error");
        }
    } else {
      return grpc::Status::OK;
    }
  }
  public:
  BlobStoreImpl(std::shared_ptr<BlobServer> blobserver) : blobserver_(blobserver) {}
  grpc::Status Read(ServerContext* context, const ReadRequest* request,
              ReadResponse* response) override {
    #ifdef debug
    std::cout << "[Read]: " << request->address() << std::endl;
    #endif
    absl::Status status = blobserver_->Read(request->address(), response->mutable_data());
    
    // Redirect to Primary by sending primary address
    if(status.code() == absl::StatusCode::kNotFound)
      response->set_primary_ip(blobserver_->get_other_ip());
    grpc::Status clientStatus = handleStatusCode(status);
    return clientStatus;
  }

  grpc::Status Write(ServerContext* context, const WriteRequest* request,
               WriteResponse* response) override {
    #ifdef debug
    std::cout << "[Write]: " << request->address() << std::endl;
    #endif
    // return StoreInternal::Write(request, response);
    absl::Status status = blobserver_->Write(request->address(), request->data());
    
    // Redirect to Primary by sending primary address
    if(status.code() == absl::StatusCode::kNotFound)
      response->set_primary_ip(blobserver_->get_other_ip());

    grpc::Status clientStatus = handleStatusCode(status);
    return clientStatus;
  }
};

class StoreInternalImpl final : public StoreInternal::Service {
  private: 
  std::shared_ptr<BlobServer>  blobserver_;
  public:
  StoreInternalImpl(std::shared_ptr<BlobServer> blobserver) : blobserver_(blobserver) {}
  grpc::Status Ping(ServerContext* context, const PingRequest* request,
              PingResponse* response) override {
    // return ::Ping(request, response);
    return grpc::Status::OK;
  }

  grpc::Status Prepare(ServerContext* context, const PrepareRequest* request,
                 PrepareResponse* response) override {
    #ifdef debug
    std::cout << "Prepare for Backup" << std::endl;
    #endif
    #ifdef performance_measure
    auto lock_acquire_start = std::chrono::high_resolution_clock::now();
    #endif

    // Acquire lock for commit to isolate request processing from recovery
    std::shared_lock<std::shared_timed_mutex> lock(blobserver_->getMutex());
    #ifdef performance_measure
    auto lock_acquire_end = std::chrono::high_resolution_clock::now();
    std::cout << "[Perf][LockAcquire]: " << std::chrono::duration_cast<std::chrono::microseconds>(lock_acquire_end - lock_acquire_start).count() << " us" << std::endl;
    #endif
    int status = blobserver_->PrepareLocal(request->address(), request->data());

    if (status != 0) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Prepare failed");
    } else {
      return grpc::Status::OK;
    }
  }

  grpc::Status Commit(ServerContext* context, const CommitRequest* request,
                CommitResponse* response) override {
    #ifdef debug
    std::cout << "Commit for Backup" << std::endl;
    #endif

    #ifdef performance_measure
    auto lock_acquire_start = std::chrono::high_resolution_clock::now();
    #endif

    // Acquire lock for commit to isolate request processing from recovery
    std::shared_lock<std::shared_timed_mutex> lock(blobserver_->getMutex());
    #ifdef performance_measure
    auto lock_acquire_end = std::chrono::high_resolution_clock::now();
    std::cout << "[Perf][LockAcquire]: " << std::chrono::duration_cast<std::chrono::microseconds>(lock_acquire_end - lock_acquire_start).count() << " us" << std::endl;
    #endif
    int status = blobserver_->CommitLocal(request->txid(), request->address());

    if (status != 0) {
      return grpc::Status(grpc::StatusCode::INTERNAL, "Commit failed");
    } else {
      return grpc::Status::OK;
    }
  }

  grpc::Status Recovery(ServerContext* context, const RecoveryRequest* request,
                  RecoveryResponse* response) override {
    // Pause writing/reading new data (Ensure no inflight requests)
    std::cout << "Acquire lock to pause all read/write requests and start recovery process" << std::endl;
    std::unique_lock<std::shared_timed_mutex> recovery_lock(blobserver_->getMutex());

    #ifdef performance_measure
    auto log_ship_start = std::chrono::high_resolution_clock::now();
    #endif

    // Set state to Primary if not already
    if(blobserver_->get_state() == BlobServerState::BACKUP) {
      blobserver_->set_state(BlobServerState::PRIMARY);
    }

    std::cout << "[Recovery]: (Primary) Received recovery request" << std::endl;
    std::vector<LogEntry> backup_logs;
    for(const LogEntry& i: request->entry() ){
      backup_logs.push_back(i);
    }
    // merge with logger, update local log
    std::cout << "[Recovery]: (Primary) Start merging log" << std::endl;
    std::vector<LogEntry> fresh_logs = blobserver_->MergeAndRefreshLogsLocal(backup_logs); //Removing earlier log, considering one server is up untill recovery

    #ifdef performance_measure
    auto merge_and_refresh_logs_end = std::chrono::high_resolution_clock::now();
    auto create_recovery_records_start = std::chrono::high_resolution_clock::now();
    #endif

    // Step 3: Create response structure to send to backup and send logs
    //Create response with files - data from fresh_logs
    std::cout << "[Recovery]: (Primary) Create Response Records" << std::endl;
    std::vector<RecoveryRecord> recovery_records = blobserver_->CreateRecoveryResponse(fresh_logs);

    response->mutable_records()->Assign(recovery_records.begin(), recovery_records.end());
    #ifdef debug
    std::cout << "[Recovery]: (Primary) Send Recovery Response: " << recovery_records.size() << " log records." << std::endl;
    #endif
    // Set other server state(backup) to be alive
    blobserver_->setBackupAlive(true);

    #ifdef performance_measure
    auto create_recovery_records_end = std::chrono::high_resolution_clock::now();
    std::cout << "[Perf][MergeRefreshLogs]: " << std::chrono::duration_cast<std::chrono::milliseconds>(merge_and_refresh_logs_end - log_ship_start).count() << " ms" << std::endl;
    std::cout << "[Perf][CreateRecoveryRecords]: " << std::chrono::duration_cast<std::chrono::milliseconds>(create_recovery_records_end - create_recovery_records_start).count() << " ms" << std::endl;
    #endif

    std::cout << "Releasing the recovery lock to allow normal request processing" << std::endl;
    return grpc::Status::OK;
  }

};

void StartBlockingServer(std::shared_ptr<grpc::Server> server){
  std::cout << "Starting Server" << std::endl;
  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

void InitializeServer(std::shared_ptr<BlobServer> blobserver){
  std::cout << "Run initialization procedure for blob server" << std::endl;
  // Initialize the BlobServer
  blobserver->ServerInit();
}

void RunServer(std::string server_address, std::string other_address, std::string root_dir_path) {
  std::shared_ptr<BlobServer> blobserver(new BlobServer(root_dir_path, server_address, other_address));
  BlobStoreImpl blobstore_service(blobserver);
  StoreInternalImpl store_internal_service(blobserver);

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&blobstore_service);
  builder.RegisterService(&store_internal_service);
  // Finally assemble the server.
  std::shared_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  std::cout << "Press Ctrl+C to quit." << std::endl;
  
  std::thread th1(StartBlockingServer, server);
  std::thread th2(InitializeServer, blobserver);
  th1.join();
  th2.join();
}

int main(int argc, char* argv[]) {
  if (argc != 4) {
    fprintf(stderr, "Usage: %s <self-ip:port> <other-ip:port> <root-dir-path>\n", argv[0]);
    return 1;
  }
  if ((getuid() == 0) || (geteuid() == 0)) {
    fprintf(stderr, "Running server as root can cause security issues.\n");
    return 1;
  }

  // Flush stdout to prevent buffering
  setbuf(stdout, NULL);
  
  std::string self_ip = argv[1];
  std::string other_ip = argv[2];
  std::string root_dir_path = argv[3];
  std::cout << "Self IP: " << self_ip << std::endl;
  std::cout << "Other IP: " << other_ip << std::endl;
  RunServer(self_ip, other_ip, root_dir_path);
  return 0;
}