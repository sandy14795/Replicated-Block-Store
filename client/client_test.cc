#include "blob_client.h"
#include "resources/utils.h"
#include <memory>
#include <cstdio>
#include <time.h>
#include <thread>
#include <unordered_map>
#include <shared_mutex>

// #define CRASH_TEST_DEBUG
#define BLOCK_SIZE 4096
#define SLEEP_INTERVAl_MS 1000

using namespace std;

typedef shared_timed_mutex Lock;
typedef unique_lock<Lock> WriteLock;
typedef shared_lock<Lock> ReadLock;

unordered_map<int, size_t> data_map;
hash<string> hash_fn;
string server1_address, server2_address;
int max_retry_count;
string home_dir;
Lock mtxLock;
Utils utils;

string log_prefix_ = "[ClientApp]: ";

void update_hash(int address, size_t str_hash) {
    WriteLock lock(mtxLock);
    data_map[address] = str_hash; //change to hash
}

size_t get_hash(int address) {
    ReadLock lock(mtxLock);
    return data_map[address];
}

int start_servers() {
  fprintf(stderr, "%s Starting servers\n", log_prefix_.c_str());
  
  string start_server1 = home_dir + "/bazel-bin/server/server " + server1_address + " " + server2_address + " " + home_dir + "/store1 >> " + home_dir + "/logs/server1.log  2>&1 &";
  int res = system(start_server1.c_str());
  if (res < 0) {
    fprintf(stderr, "%s Failed to start primary server.\n", log_prefix_.c_str());
    return res;
  }

  // Sleep for a while to let primary server start
  sleep(SLEEP_INTERVAl_MS/1000);
  
  string start_server2 = home_dir + "/bazel-bin/server/server " + server2_address + " " + server1_address + " " + home_dir + "/store2 >> " + home_dir + "/logs/server2.log  2>&1 &";
  res = system(start_server2.c_str());

  if (res < 0) {
    fprintf(stderr, "%s Failed to start backup server.\n", log_prefix_.c_str());
    return res;
  }

  fprintf(stderr, "%s Servers started.\n", log_prefix_.c_str());
  return 0;
}

void stop_servers() {
  fprintf(stderr, "%s Stopping servers\n", log_prefix_.c_str());

  string server_binary = home_dir + "/bazel-bin/server/server";
  string kill_servers_cmd = " ps -ef | grep " + server_binary + " | grep -v grep | awk '{print $2}' | xargs -r kill -9";
  int res = system(kill_servers_cmd.c_str());
  if (res < 0) {
    fprintf(stderr, "%s Failed to stop servers.\n", log_prefix_.c_str());
  }
}

int64_t create_crash_address(int64_t address, CrashType crash_type) {
  int64_t crash_address = crash_type * MAX_ADDRESS_LENGTH;
  int64_t address_to_write = -1 * (crash_address + address);
  return address_to_write;
}

void write_read_interleaved_clients(shared_ptr<BlobClient> client, int client_id, bool crash_server, CrashType crash_type) {
  int res;
  int num_requests = 10;
  int crash_address_idx = rand() % num_requests;
  if(crash_server) {
    fprintf(stderr, "%s Client %d: Running [write_read_interleaved_with_crash: %s]\n", log_prefix_.c_str(), client_id, Utils::crash_type_to_string(crash_type).c_str());
  } else {
    fprintf(stderr, "%s Client %d: Running [write_read_interleaved]\n", log_prefix_.c_str(), client_id);
  }

  int64_t address_to_write;
  for(int req_num = 0; req_num < num_requests; req_num++) {
    char ch = 'a' + ((req_num + client_id + 1) % 26);
    string data(BLOCK_SIZE, ch);
    int64_t address = (req_num * BLOCK_SIZE) + 3;
    
    if (crash_server && req_num == crash_address_idx) {
      address_to_write = create_crash_address(address, crash_type);
      fprintf(stderr, "%s Client %d: Address used for crashing: %ld\n", log_prefix_.c_str(), client_id, address_to_write);
    } else {
      address_to_write = address;
    }

    this_thread::sleep_for(chrono::milliseconds(rand() % SLEEP_INTERVAl_MS));
    res = client->write(address_to_write, data);

    if (res < 0) {
      fprintf(stderr, "%s Failed to write to server.\n", log_prefix_.c_str());
    } else {
      size_t str_hash = hash_fn(data);
      #ifdef CRASH_TEST_DEBUG
      fprintf(stderr, "%s Client %d: Wrote %d bytes to server at address %ld, Hash: %ld\n", log_prefix_.c_str(), client_id, res, address, str_hash);
      #endif
      update_hash(address, str_hash);
    }

    this_thread::sleep_for(chrono::milliseconds(rand() % SLEEP_INTERVAl_MS));
    res = client->read(address_to_write, data);

    if (res < 0) {
      fprintf(stderr, "%s Failed to read from server. Error code: %d\n", log_prefix_.c_str(), res);
    } else {
      size_t str_hash = hash_fn(data);
      size_t expected_hash = get_hash(address);
      #ifdef CRASH_TEST_DEBUG
      fprintf(stderr, "%s Client %d: Read %d bytes from server at address %ld, Hash: %ld\n", log_prefix_.c_str(), client_id, res, address, str_hash);
      #endif
      if (str_hash != expected_hash) {
        fprintf(stderr, "%s Client %d: Read data does not match at address %ld, Hash: %ld, Expected Hash %ld. Exiting...\n", log_prefix_.c_str(), client_id, address, str_hash, expected_hash);
        return; 
      }
    }
  }
}

int test_multiple_clients_with_crash(int num_clients, CrashType crash_type) {
  int res = start_servers();
  if (res < 0) {
    fprintf(stderr, "%s Failed to start servers.\n", log_prefix_.c_str());
    return res;
  }

  sleep(2);

  // Create multiple clients and connect to server
  shared_ptr<BlobClient> clients[num_clients];
  for(int i=0; i<num_clients; i++) {
    clients[i] = shared_ptr<BlobClient>(new BlobClient(server1_address, server2_address, max_retry_count));
    clients[i]->connect();
  }

  int client_idx_to_crash = rand() % num_clients;
  thread threads[num_clients];
  
  // Spawn multiple threads:
  for (int i=0; i<num_clients; ++i){
    bool crash_server = (i == client_idx_to_crash);
    threads[i] = thread(write_read_interleaved_clients,clients[i],i, crash_server, crash_type);
  }

  for (auto& th : threads){
    th.join();
  }

  stop_servers();
  return 0;
}

int main(int argc, char* argv[]) {
  if (argc != 2) {
    fprintf(stderr, "Usage: %s <conf file>\n", argv[0]);
    return 1;
  }

  string conf_file = argv[1];
  srand(time(0));

  if(utils.parse_config_file(conf_file) != 0) {
    fprintf(stderr, "%s Failed to parse config file.\n", log_prefix_.c_str());
    return -1;
  }

  server1_address = utils.config["server1_address"], server2_address = utils.config["server2_address"];
  max_retry_count = atoi(utils.config["max_retry_count"].c_str());
  home_dir = utils.config["home_dir"];

  stop_servers();

  // Remove log folder
  string rm_log_folder_cmd = "rm -rf " + home_dir + "/logs/server*";
  system(rm_log_folder_cmd.c_str());

  int num_clients = 10;
  vector<CrashType> crash_types = {
    BACKUP_CRASH_AFTER_PRIMARY_PREPARE, // Write crash
    PRIMARY_CRASH_AFTER_LOCAL_PREPARE,  // Write crash
    PRIMARY_CRASH_AFTER_LOCAL_COMMIT,   // Write crash
    BACKUP_CRASH_AFTER_PRIMARY_COMMIT,  // Write crash
    PRIMARY_CRASH_BEFORE_READ           // Read crash
  };

  for(CrashType crash_type : crash_types) {
    fprintf(stderr, "%s ============ RUNNING CORRECTNESS TEST | CRASH TYPE: %s ============\n", log_prefix_.c_str(), Utils::crash_type_to_string(crash_type).c_str());
    test_multiple_clients_with_crash(num_clients, crash_type);
  }

  fprintf(stderr, "%s ============ TESTS COMPLETED ============\n", log_prefix_.c_str());
  return 0;
}