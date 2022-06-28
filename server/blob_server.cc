#include "blob_server.h"
#include <string> 
#include <memory>
#include <assert.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>
#include <signal.h>

#include <sys/stat.h>
#include <sys/types.h>

#include <grpcpp/grpcpp.h>

#ifdef BAZEL_BUILD
#else
#include "protos/blobstore.grpc.pb.h"
#endif

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "resources/utils.h"

#define BLOCK_SIZE 4096
// #define performance_measure

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
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

BlobServer::BlobServer(std::string root_path, 
                    std::string self_ip, 
                    std::string other_ip): 
                    root_path_(root_path), 
                    self_ip_(self_ip), 
                    other_ip_(other_ip) {
  srand(time(0));
  // Initialize storage directories
  ::mkdir(this->root_path_.c_str(), 0777);
  this->tmp_path_ = this->root_path_ + "/tmp";
  ::mkdir(this->tmp_path_.c_str(), 0777);
  // Initialize logger
  logger_ = std::make_shared<Logger>(this->root_path_ + "/log");
  // Connect to other storage server.
  ConnectToOtherBlobServer();
}

absl::Status BlobServer::Recovery(){
  //read backup logs and send to primary
  std::cout << "[Recovery]: (Backup) Send Recovery request" << std::endl;
  std::vector<LogEntry> self_logs = logger_->read_logs();
  std::cout << "Read " << self_logs.size() << " log entries on local" << std::endl;
  RecoveryRequest recovery_request;
  RecoveryResponse recovery_response;
  recovery_request.mutable_entry()->Assign(self_logs.begin(), self_logs.end());
  #ifdef performance_measure
  auto log_import_start = std::chrono::high_resolution_clock::now();
  #endif
  // Step 1: Send backup logs to primary
  grpc::Status status = store_internal_client_->Recovery(recovery_request, &recovery_response);

  #ifdef performance_measure
  auto log_import_end = std::chrono::high_resolution_clock::now();
  #endif

  if (status.ok()) {
    // to_be_implemented
    // clear log file and iterate over recovery_response to add log entries and write data
    // Debug_Print - Need to disable. 
    // Step 4: Replay log records and restore the state

    std::cout << "[Backup] Number of Log entries to be replayed: " << recovery_response.records().size() << std::endl;

    #ifdef performance_measure
    auto log_replay_start = std::chrono::high_resolution_clock::now();
    #endif

    int replay_status = ReplayRecoveryRecords(recovery_response);
    std::cout << "Replay Status: " << replay_status << std::endl;

    #ifdef performance_measure
    auto log_replay_end = std::chrono::high_resolution_clock::now();
    std::cout << "[Perf][LogImport]: " << std::chrono::duration_cast<std::chrono::milliseconds>(log_import_end - log_import_start).count() << " ms" << std::endl;
    std::cout << "[Perf][LogReplay]: " << std::chrono::duration_cast<std::chrono::milliseconds>(log_replay_end - log_replay_start).count() << " ms" << std::endl;
    #endif

    #ifdef debug
    for(auto it = recovery_response.records().begin(); it != recovery_response.records().end(); it++){
      std::cout<< "[Recovery]: (Backup) Recovery record: " << it->entry().txid() << std::endl;
    }
    #endif

    std::cout << "Backup Recovery successful." << std::endl;
    return absl::OkStatus();
  } else {
    std::cout << "Backup Recovery failed." << std::endl;
    return absl::CancelledError();
  }
 
}

void writeToTmpFile(std::string file_path, const std::string& data){
  std::ofstream of(file_path, std::ios::trunc | std::ios::out);
  of << data;
  of.close();
}

int BlobServer::ReplayRecoveryRecords(RecoveryResponse& recovery_response) {
  // Replay logs:
  // 0. Truncate old log file
  // For each record:
  // 1. Create tmp file
  // 2. Add entry to log
  // 3. Rename tmp file(s) to file(s)

  // 0. Clear old log file
  this->logger_->clear_logs();

  #ifdef debug
  std::cout << "[Backup] Number of records in log before replay: " << this->logger_->read_logs().size() << std::endl;
  std::cout << "[Backup] Records Replayed: " << recovery_response.records().size() << std::endl;
  #endif

  for(auto record = recovery_response.records().begin(); record != recovery_response.records().end(); record++) {
    LogEntry entry = record->entry();

    // 1. Create tmp file
    std::string file_path1 = GetFilePath(this->tmp_path_, entry.address1());
    std::string file_path2 = GetFilePath(this->tmp_path_, entry.address2());
    
    writeToTmpFile(file_path1, record->data1());
    if(entry.address2() != -1){
      writeToTmpFile(file_path2, record->data2());
    }

    // 2. Add entry to log
    int rc = this->logger_->add_entry(entry.txid(), entry.address1(), entry.address2(), entry.status());
    if(rc < 0)
      return -1;

    std::string original_file_path1 = GetFilePath(this->root_path_, entry.address1());
    std::string original_file_path2 = GetFilePath(this->root_path_, entry.address2());

    // 3. Rename tmp files to actual files
    std::rename(file_path1.c_str(), original_file_path1.c_str());
    if(entry.address2() != -1){
      std::rename(file_path2.c_str(), original_file_path2.c_str());
    }
  }
  #ifdef debug
  std::cout << "[Backup] Number of records in log after replay: " << this->logger_->read_logs().size() << std::endl;
  #endif

  return 0;
}

void BlobServer::ConnectToOtherBlobServer() {
  // Connect to other storage server.
  grpc::ChannelArguments ch_args;
    ch_args.SetMaxReceiveMessageSize(-1);
    auto channel = grpc::CreateCustomChannel(other_ip_, grpc::InsecureChannelCredentials(), ch_args);
    store_internal_client_ = std::move(std::make_unique<StoreInternalClient>(channel));
}

void BlobServer::ServerInit() {
  std::cout << "BlobServer::ServerInit()" << std::endl;
  std::unique_lock<std::shared_timed_mutex> recovery_lock(recovery_mutex_);
  // Ping other storage server.
  PingResponse ping_response;
  PingRequest ping_request;
  grpc::Status status = store_internal_client_->Ping(ping_request, &ping_response);
  if (status.ok()) {
    std::cout << "Ping other storage server successfully." << std::endl;
    this->state = BACKUP;
    backupAlive = true;
    #ifdef performance_measure
    auto recovery_start = std::chrono::high_resolution_clock::now();
    #endif
    absl::Status status = Recovery();
    #ifdef performance_measure
    auto recovery_end = std::chrono::high_resolution_clock::now();
    std::cout << "[Perf][Recovery]: " << std::chrono::duration_cast<std::chrono::milliseconds>(recovery_end - recovery_start).count() << " ms" << std::endl;
    #endif
    if(status.ok()){
      std::cout << "Backup Recovery successful." << std::endl;
    } else{
      std::cout << "Backup Recovery failed." << std::endl;
      // Exit if recovery failed.
      exit(1);
    }
  } else {
    std::cout << "Ping other storage server failed." << std::endl;
    this->state = PRIMARY;
    backupAlive = false;
  }
}

std::vector<LogEntry> BlobServer::MergeAndRefreshLogsLocal(std::vector<LogEntry>& backup_logs){
  #ifdef performance_measure
  auto log_merge_start = std::chrono::high_resolution_clock::now();
  #endif
  // Recovery Step 2: Merge logs to create logs to send to backup
  std::vector<LogEntry> fresh_logs = logger_->merge_logs(backup_logs);
  #ifdef performance_measure
  auto log_merge_end = std::chrono::high_resolution_clock::now();
  auto refresh_logs_start = std::chrono::high_resolution_clock::now();
  #endif
  // Auxilliary: Clear the unnecessary logs on primary
  int status = logger_->refresh_logs(fresh_logs);

  #ifdef performance_measure
  auto refresh_logs_end = std::chrono::high_resolution_clock::now();
  std::cout << "[Perf][LogMerge]: " << std::chrono::duration_cast<std::chrono::microseconds>(log_merge_end - log_merge_start).count() << " us" << std::endl;
  std::cout << "[Perf][RefreshLogs]: " << std::chrono::duration_cast<std::chrono::microseconds>(refresh_logs_end - refresh_logs_start).count() << " us" << std::endl;
  #endif

  return fresh_logs;

}

std::vector<RecoveryRecord> BlobServer::CreateRecoveryResponse(std::vector<LogEntry>& fresh_logs){
  //Populate response with log entries and data
  std::vector<RecoveryRecord> recovery_records;

  for(auto& log_entry : fresh_logs){
    RecoveryRecord recovery_record;

    LogEntry *logentry = recovery_record.mutable_entry();
    logentry->CopyFrom(log_entry); //copy log entry

    std::ifstream file(this->root_path_ + "/" + std::to_string(log_entry.address1()));
    std::stringstream buffer;
    buffer << file.rdbuf();
    recovery_record.set_data1(buffer.str());
    file.close();
    
    // Clear stringstream: Not buffer.clear()
    buffer.str("");

    if(log_entry.address2() != -1){
      file.open(this->root_path_ + "/" + std::to_string(log_entry.address2()));
      buffer << file.rdbuf();
      recovery_record.set_data2(buffer.str());
      file.close();
    }
    recovery_records.push_back(recovery_record);
  }
  return recovery_records;
}

int BlobServer::PrepareLocal(int64_t addr, const std::string& data) {
  #ifdef performance_measure
  auto prepare_local_start = std::chrono::high_resolution_clock::now();
  #endif
  #ifdef CRASH_TEST
  CrashType crash_type = Utils::get_crash_type(addr);
  if(state == BACKUP && crash_type == CrashType::BACKUP_CRASH_AFTER_PRIMARY_PREPARE) {
    printf("[Backup] Prepare: %s.\n", Utils::crash_type_to_string(crash_type).c_str());
    kill(getpid(), SIGKILL);
  }
  int64_t address = Utils::get_address(addr);
  #else
  int64_t address = addr;
  #endif

  int actual_address1 = address / BLOCK_SIZE;
  int actual_address2 = (address % BLOCK_SIZE == 0) ? -1 : actual_address1 + 1;
  
  if(address % BLOCK_SIZE == 0) {
    // Directly write BLOCK_SIZE bytes to actual address.
    std::string tmp_file_path = GetFilePath(this->tmp_path_, actual_address1);
    std::ofstream tmp_file(tmp_file_path);
    tmp_file << data;
    tmp_file.close();
  } else {
    // Operations for actual_address1 -
    // Read current data from actual address1

    std::string file_path1 = GetFilePath(this->root_path_, actual_address1);
    std::string buffer;
    auto file = std::ifstream(file_path1);
    if(file.is_open()) {
      file >> buffer;
      file.close();
    }
    // Pad the buffer with spaces.
    buffer.resize(BLOCK_SIZE, ' ');

    // Write part of new data to buffer at offset
    int offset = address % BLOCK_SIZE;
    int len = BLOCK_SIZE - (address % BLOCK_SIZE);
    int pos = 0;
    buffer.replace(offset, len, data, pos, len);
    
    // Write buffer to tmp file for actual address1
    std::string tmp_file_path1 = GetFilePath(this->tmp_path_, actual_address1);
    auto tmp_file = std::ofstream(tmp_file_path1);
    tmp_file << buffer;
    tmp_file.close();
    
    // Operations for actual_address2 -
    // Read current data from actual address2
    std::string file_path2 = GetFilePath(this->root_path_, actual_address2);
    buffer.clear();
    file = std::ifstream(file_path2);
    if(file.is_open()) {
      file >> buffer;
      file.close();
    }

    // Pad the buffer with spaces.
    buffer.resize(BLOCK_SIZE, ' ');

    // Write remaining data to buffer at beginning
    offset = 0;
    len = address % BLOCK_SIZE;
    pos = BLOCK_SIZE - (address % BLOCK_SIZE);
    buffer.replace(offset, len, data, pos, len);
    
    // Write buffer to tmp file for actual address2
    std::string tmp_file_path2 = GetFilePath(this->tmp_path_, actual_address2);
    tmp_file = std::ofstream(tmp_file_path2);
    tmp_file << buffer;
    tmp_file.close();
  }
    #ifdef performance_measure
    auto prepare_local_end = std::chrono::high_resolution_clock::now();
    std::cout << "[Perf][PrepareLocal]: " << std::chrono::duration_cast<std::chrono::microseconds>(prepare_local_end - prepare_local_start).count() << " us" << std::endl;
    #endif

  return 0;
}

int BlobServer::Prepare(int64_t address, const std::string& data) {
  #ifdef debug
  std::cout << "Starting Prepare for addr: " << address << ", data size: " << data.size() << std::endl;
  #endif

  #ifdef performance_measure
  auto prepare_start = std::chrono::high_resolution_clock::now();
  #endif

  int localStatus = PrepareLocal(address, data);
  if (localStatus != 0) {
    std::cout << "Prepare for addr: " << address << ", data size: " << data.size() << " failed." << std::endl;
    return localStatus;
  }

  #ifdef performance_measure
  auto prepare_remote_start = std::chrono::high_resolution_clock::now();
  #endif
  
  #ifdef CRASH_TEST
  CrashType crash_type = Utils::get_crash_type(address);
  if(state == PRIMARY && crash_type == CrashType::PRIMARY_CRASH_AFTER_LOCAL_PREPARE) {
    printf("[Primary] Prepare: %s.\n", Utils::crash_type_to_string(crash_type).c_str());
    kill(getpid(), SIGKILL);
  }
  #endif

  // Don't send prepare to backup if it is not alive.
  if(!backupAlive) return 0;


  #ifdef debug
  std::cout << "PrepareLocal succeeded." << std::endl;
  std::cout << "[BlobServer::Prepare] Preparing on other server" << std::endl;
  #endif

  // Prepare in backup storage server.
  PrepareRequest prepare_request;
  PrepareResponse prepare_response;
  prepare_request.set_address(address);
  prepare_request.set_data(data);
  grpc::Status status = store_internal_client_->Prepare(prepare_request, &prepare_response);
  if (status.ok()) {
    #ifdef debug
    std::cout << "Prepare Remote successful." << std::endl;
    #endif
  } else {
    std::cout << "Prepare Remote failed." << std::endl;
    // Failure is assumed to be Backup Failure.
    backupAlive = false;
  }
  #ifdef performance_measure
  auto prepare_remote_end = std::chrono::high_resolution_clock::now();
  std::cout << "[Perf][PrepareRemote]: " << std::chrono::duration_cast<std::chrono::microseconds>(prepare_remote_end - prepare_remote_start).count() << " us" << std::endl;
  std::cout << "[Perf][Prepare]: " << std::chrono::duration_cast<std::chrono::microseconds>(prepare_remote_end - prepare_start).count() << " us" << std::endl;
  #endif

  return 0;
}

int BlobServer::CommitLocal(int64_t txId, int64_t addr) {
  #ifdef performance_measure
  auto commit_local_start = std::chrono::high_resolution_clock::now();
  #endif

  #ifdef CRASH_TEST  
  CrashType crash_type = Utils::get_crash_type(addr);
  if(state == BACKUP && crash_type == CrashType::BACKUP_CRASH_AFTER_PRIMARY_PREPARE) {
    printf("[Backup] Commit: %s.\n", Utils::crash_type_to_string(crash_type).c_str());
    kill(getpid(), SIGKILL);
  }
  int64_t address = Utils::get_address(addr);
  #else
  int64_t address = addr;
  #endif

  int actual_address1 = address / BLOCK_SIZE;
  int actual_address2 = (address % BLOCK_SIZE == 0) ? -1 : actual_address1 + 1;
  
  #ifdef debug
  printf("CommitLocal for addr: %ld -> txId: %ld, actual_addr1: %d, actual_addr2: %d\n",
      address, txId, actual_address1, actual_address2);
  #endif
  
  logger_->add_entry(txId, actual_address1, actual_address2, 1);

  #ifdef performance_measure
  auto add_log_entry_end = std::chrono::high_resolution_clock::now();
  auto rename_start = std::chrono::high_resolution_clock::now();
  #endif

  if(address % BLOCK_SIZE == 0) {
    std::string tmp_file_path = GetFilePath(this->tmp_path_, actual_address1);
    std::string file_path = GetFilePath(this->root_path_, actual_address1);
    std::rename(tmp_file_path.c_str(), file_path.c_str());
  } else {
    std::string tmp_file_path1 = GetFilePath(this->tmp_path_, actual_address1);
    std::string file_path1 = GetFilePath(this->root_path_, actual_address1);
    std::rename(tmp_file_path1.c_str(), file_path1.c_str());

    // TODO: Try to handle atomic renaming of both tmp files.
    std::string tmp_file_path2 = GetFilePath(this->tmp_path_, actual_address2);
    std::string file_path2 = GetFilePath(this->root_path_, actual_address2);
    std::rename(tmp_file_path2.c_str(), file_path2.c_str());
  }

  #ifdef performance_measure
  auto rename_end = std::chrono::high_resolution_clock::now();
  auto commit_local_end = std::chrono::high_resolution_clock::now();
  std::cout << "[Perf][AddLogEntry]: " << std::chrono::duration_cast<std::chrono::microseconds>(add_log_entry_end - commit_local_start).count() << " us" << std::endl;
  std::cout << "[Perf][Rename]: " << std::chrono::duration_cast<std::chrono::microseconds>(rename_end - rename_start).count() << " us" << std::endl;
  std::cout << "[Perf][CommitLocal]: " << std::chrono::duration_cast<std::chrono::microseconds>(commit_local_end - commit_local_start).count() << " us" << std::endl;
  #endif
      #ifdef performance_measure
    
    #endif

  return 0;
}

int64_t BlobServer::generate_txId() {
  return ((int64_t) rand()) << 32 | (int64_t) rand();
}

int BlobServer::Commit(int64_t address) {
  int64_t txId = generate_txId();
  #ifdef debug
  std::cout << "Starting Commit for addr: " << address << "txId: " << txId << std::endl;
  #endif

  #ifdef performance_measure
  auto commit_start = std::chrono::high_resolution_clock::now();
  #endif

  int localStatus = CommitLocal(txId, address);
  if (localStatus != 0) {
    std::cout << "CommitLocal[address: " << address << ", txId: " << txId << " ] failed." << std::endl;
    return localStatus;
  }

  #ifdef performance_measure
  auto commit_remote_start = std::chrono::high_resolution_clock::now();
  #endif

  #ifdef CRASH_TEST  
  CrashType crash_type = Utils::get_crash_type(address);
  if(state == PRIMARY && crash_type == CrashType::PRIMARY_CRASH_AFTER_LOCAL_COMMIT) {
    printf("[Primary] Commit: %s.\n", Utils::crash_type_to_string(crash_type).c_str());
    kill(getpid(), SIGKILL);
  }
  #endif

  if(!backupAlive) return 0;

  // Commit in backup storage server.
  CommitRequest commit_request;
  CommitResponse commit_response;
  commit_request.set_txid(txId);
  commit_request.set_address(address);
  grpc::Status status = store_internal_client_->Commit(commit_request, &commit_response);
  if (status.ok()) {
    #ifdef debug
    std::cout << "Commit Remote successful." << std::endl;
    #endif
  } else {
    std::cout << "Commit Remote failed." << std::endl;
    // Failure is assumed to be Backup Failure.
    backupAlive = false;
  }

  #ifdef performance_measure
  auto commit_remote_end = std::chrono::high_resolution_clock::now();
  std::cout << "[Perf][CommitRemote]: " << std::chrono::duration_cast<std::chrono::microseconds>(commit_remote_end - commit_remote_start).count() << " us" << std::endl;
  std::cout << "[Perf][Commit]: " << std::chrono::duration_cast<std::chrono::microseconds>(commit_remote_end - commit_start).count() << " us" << std::endl;
  #endif

  return 0;
}

std::string BlobServer::GetFilePath(std::string root, int64_t address) {
  std::string file_name = std::to_string(address);
  std::string file_path = root + "/" + file_name;
  return file_path;
}

bool BlobServer::CheckPrimaryFailure() {
  // Check if primary is alive
  PingRequest ping_request;
  PingResponse ping_response;
  grpc::Status primaryStatus = store_internal_client_->Ping(ping_request, &ping_response);
  if(primaryStatus.ok()){
    return false;
  } else { // Primary is dead, Backup becomes the new Primary
    return true;
  }
}


absl::Status BlobServer::Read(int64_t addr, std::string* data) {
  #ifdef performance_measure
  auto lock_acquire_start = std::chrono::high_resolution_clock::now();
  #endif
  std::shared_lock<std::shared_timed_mutex> recovery_lock(this->recovery_mutex_);
  std::shared_lock<std::shared_timed_mutex> read_lock(this->mutex_);

  #ifdef performance_measure
  auto lock_acquire_end = std::chrono::high_resolution_clock::now();
  std::cout << "[Perf][LockAcquire]: " << std::chrono::duration_cast<std::chrono::microseconds>(lock_acquire_end - lock_acquire_start).count() << " us" << std::endl;
  #endif

  #ifdef debug
  std::cout << "[BlobServer::Read] " << addr << std::endl;
  #endif

  #ifdef performance_measure
  auto read_start = std::chrono::high_resolution_clock::now();
  #endif

  if(this->state == BACKUP){
    bool primaryFailure = CheckPrimaryFailure();
    if(!primaryFailure) {
      absl::string_view err_msg("Please contact primary.");
      return absl::NotFoundError(err_msg);
    }
    std::cout << "[Backup] Primary failure detected. Taking over as the new Primary." << std::endl;
    this->state = PRIMARY;
    backupAlive = false;
  }

  #ifdef CRASH_TEST
  CrashType crash_type = Utils::get_crash_type(addr);
  if(state == PRIMARY && crash_type == CrashType::PRIMARY_CRASH_BEFORE_READ) {
    printf("[Primary] Read: %s.\n", Utils::crash_type_to_string(crash_type).c_str());
    kill(getpid(), SIGKILL);
  }
  int64_t address = Utils::get_address(addr);
  #else
  int64_t address = addr;
  #endif

  // aligned read
  if(address % BLOCK_SIZE == 0){
    std::string file_path = GetFilePath(this->root_path_, address / BLOCK_SIZE);
    std::ifstream file(file_path);
    std::stringstream buffer;
    buffer << file.rdbuf();
    *data = buffer.str();
  } else { // unaligned read
    std::string file_path1 = GetFilePath(this->root_path_, address / BLOCK_SIZE);
    std::ifstream file1(file_path1);
    std::stringstream buffer1;
    buffer1 << file1.rdbuf();
    std::string data1 = buffer1.str();

    std::string file_path2 = GetFilePath(this->root_path_, address / BLOCK_SIZE + 1);
    std::ifstream file2(file_path2);
    std::stringstream buffer2;
    buffer2 << file2.rdbuf();
    std::string data2 = buffer2.str();

    int offset = address % BLOCK_SIZE;
    int len = BLOCK_SIZE - offset;
    *data = data1.substr(offset, len) + data2.substr(0, offset);
  }

  #ifdef performance_measure
  auto read_end = std::chrono::high_resolution_clock::now();
  std::cout << "[Perf][Read]: " << std::chrono::duration_cast<std::chrono::microseconds>(read_end - read_start).count() << " us" << std::endl;
  #endif

  return absl::OkStatus();
}

absl::Status BlobServer::Write(int64_t address, const std::string& data) {
  // Data size must be a fixed size
  assert(data.size() == BLOCK_SIZE);
  #ifdef performance_measure
  auto lock_acquire_start = std::chrono::high_resolution_clock::now();
  #endif
  std::shared_lock<std::shared_timed_mutex> recovery_lock(this->recovery_mutex_);
  #ifdef debug
  std::cout << "[BlobServer::Write()]: " << address << std::endl;
  #endif

  #ifdef performance_measure
  auto lock_acquire_end = std::chrono::high_resolution_clock::now();
  auto write_start = std::chrono::high_resolution_clock::now();
  std::cout << "[Perf][LockAcquire]: " << std::chrono::duration_cast<std::chrono::microseconds>(lock_acquire_end - lock_acquire_start).count() << " us" << std::endl;
  #endif

  {
    int64_t block_address = address / BLOCK_SIZE;
    int id1 = block_address % NUM_MUTEXES, id2 = (block_address + 1) % NUM_MUTEXES;
    if(id1 > id2)
        std::swap(id1, id2);

    // Acquire locks for both adjacent blocks
    std::unique_lock<std::mutex> lock1(this->mutex_pool_[id1]);
    std::unique_lock<std::mutex> lock2(this->mutex_pool_[id2]);
    
    if(this->state == BACKUP) {
      bool primaryFailure = CheckPrimaryFailure();
      if(!primaryFailure) {
        absl::string_view err_msg("Please contact primary.");
        return absl::NotFoundError(err_msg);
      }
      std::cout << "[Backup] Primary failure detected. Taking over as the new Primary." << std::endl;
      this->state = PRIMARY;
      backupAlive = false;
    }
    
    int rc = Prepare(address, data);
    if(rc != 0){
      std::cout << "[Write]: " << address << ", Prepare failure: " << rc << std::endl;
      return absl::CancelledError();
   }
  }

  // Coarse-grained locking for commit
  std::unique_lock<std::shared_timed_mutex> write_lock(this->mutex_);

  int rc = Commit(address);
  #ifdef performance_measure
  auto write_end = std::chrono::high_resolution_clock::now();
  std::cout << "[Perf][Write]: " << std::chrono::duration_cast<std::chrono::microseconds>(write_end - write_start).count() << " us" << std::endl;
  #endif
  
  if(rc != 0){
    std::cout << "[Write]: " << address << ", Commit failure: " << rc << std::endl;
    return absl::CancelledError();
  }

  return absl::OkStatus();
}