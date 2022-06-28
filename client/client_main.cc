#include "blob_client.h"
#include "resources/utils.h"
#include <memory>
#include <cstdio>
#include <time.h>
#include <iostream>

// #define debug

// #define debug

using namespace std;

unique_ptr<BlobClient> client;

void read_write_unaligned() {
  printf("Running [read_write_unaligned]\n");
  int res;
  int num_requests = 10000;
  for(int address = 0; address < num_requests; address++) {
    char ch = 'a' + (rand() % 26);
    string data(4096, ch);

    res = client->write(address, data);
    
    if (res < 0) {
      printf("Failed to write to server.\n");
    } else {
      #ifdef debug
      printf("Wrote %d bytes to server.\n", res);
      #endif
    }
    

    string data2(4096, '\0');
    res = client->read(address, data2);
    
    if (res < 0) {
      printf("Failed to read from server. Error code: %d\n", res);
    } else {
      #ifdef debug
      printf("Read %d bytes from server.\n", res);
      #endif
    }
    
    // printf("Data: %s\n", data.c_str());
  }
}

void read_write_aligned() {
  printf("Running [read_write_aligned]\n");
  int res;
  int num_requests = 10000;
  for(int req_num = 0; req_num < num_requests; req_num++) {
    char ch = 'a' + (req_num % 26);
    string data(4096, ch);
    int address = req_num * 4096;

    res = client->write(address, data);
    #ifdef debug
    if (res < 0) {
      printf("Failed to write to server.\n");
    } else {
      printf("Wrote %d bytes to server.\n", res);
    }
    #endif

    data.clear();
    data.resize(4096, '\0');
    res = client->read(address, data);
    #ifdef debug
    if (res < 0) {
      printf("Failed to read from server. Error code: %d\n", res);
    } else {
      printf("Read %d bytes from server.\n", res);
    }
    #endif
    // printf("Data: %s\n", data.c_str());
  }
}

int64_t get_address_with_crash(int64_t address, CrashType crash_type) {
  int64_t crash_address = crash_type * MAX_ADDRESS_LENGTH;
  int64_t address_to_write = -1 * (crash_address + address);
  printf("Crashing at address: %ld\n", address_to_write);
  return address_to_write;
}

void read_write_with_crash(CrashType crash_type) {
  printf("Running [read_write_with_crash: %s]\n", Utils::crash_type_to_string(crash_type).c_str());
  int res;
  int num_requests = 10;
  int crash_address_idx = rand() % num_requests;
  printf("Crash address index: %d\n", crash_address_idx);
  int64_t address_to_write;
  for(int address = 0; address < num_requests; address++) {
    char ch = 'a' + (rand() % 26);
    string data(4096, ch);
    if (address == crash_address_idx) {
      address_to_write = get_address_with_crash(address, crash_type);
    } else {
      address_to_write = address;
    }
    res = client->write(address_to_write, data);
    if (res < 0) {
      printf("Failed to write to server.\n");
    } else {
      #ifdef debug 
      printf("Wrote %d bytes to server.\n", res);
      #endif
    }

    data.clear();
    data.resize(4096, '\0');
    res = client->read(address_to_write, data);
    if (res < 0) {
      printf("Failed to read from server. Error code: %d\n", res);
    } else {
      #ifdef debug
      printf("Read %d bytes from server.\n", res);
      #endif
    }

    // printf("Data: %s\n", data.c_str());
  }
}


int main(int argc, char* argv[]) {
  if (argc != 2) {
    printf("Usage: %s <conf file>\n", argv[0]);
    return 1;
  }

  string conf_file = argv[1];
  Utils utils;

  if(utils.parse_config_file(conf_file) != 0) {
    std::cout << "Failed to parse config file" << std::endl;
    return -1;
  }

  string server1_address = utils.config["server1_address"], server2_address = utils.config["server2_address"];
  int max_retry_count = atoi(utils.config["max_retry_count"].c_str());
  srand(time(0));

  // Create BlobClient and connect to server
  client = unique_ptr<BlobClient>(new BlobClient(server1_address, server2_address, max_retry_count));
  client->connect();
  
  // Run read_write_unaligned
  struct timeval start, end;
  double time_taken;
  gettimeofday(&start, NULL);
  read_write_unaligned();
  gettimeofday(&end, NULL);
  time_taken = (end.tv_sec - start.tv_sec) * 1e6 + (end.tv_usec - start.tv_usec);
  printf("Time taken [read_write_unaligned]: %f\n", time_taken);

  // Run crash tests - See list of crashes in CrashType enum
  read_write_with_crash(CrashType::PRIMARY_CRASH_AFTER_LOCAL_COMMIT);

  printf("Completed all tests.\n");
}