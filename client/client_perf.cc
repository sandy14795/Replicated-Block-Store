#include "blob_client.h"
#include "resources/utils.h"
#include <iostream>
#include <string>
#include <type_traits>
#include <thread>
#include <chrono>
#include <mutex>
#include <time.h>
#include <unistd.h>
#include <random>

#include <condition_variable>
#include "absl/flags/flag.h"
#include "absl/flags/marshalling.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"

enum { NS_PER_SECOND =  1000000000 };
ABSL_FLAG(std::string , key_distribution, "uniform",
          "Key distribution: uniform, exponential (3)");
ABSL_FLAG(float, write_ratio, 0.5,
          "Ratio of writes to total requests");
ABSL_FLAG(int64_t, store_size, 256 * 1024,
          "Storage size in MBs"); // default is 1 GB.
ABSL_FLAG(int, num_clients, 10, "Number of clients");
ABSL_FLAG(std::string, alignment, "aligned",
          "Alignment of data: aligned or unaligned");
ABSL_FLAG(int, requests_per_client, 5000, "Number of requests per client"); 
// default is a million per client
ABSL_FLAG(std::string, config_file, "/mnt/Work/CS739-P3/resources/exec.conf", "Path to config file");

double sub_timespec(timespec* start_time, timespec* end_time, timespec* delta) {
  delta->tv_nsec = end_time->tv_nsec - start_time->tv_nsec;
  delta->tv_sec = end_time->tv_sec - start_time->tv_sec;
  if (delta->tv_nsec < 0 && delta->tv_sec > 0) {
    delta->tv_nsec += NS_PER_SECOND;
    delta->tv_sec--;
  }    
  if (delta->tv_nsec > 0 && delta->tv_sec < 0) {
    delta->tv_nsec -= NS_PER_SECOND;
    delta->tv_sec++;
  }
  return delta->tv_sec*NS_PER_SECOND + delta->tv_nsec;   
}

void clear_caches() {
  // system("sync; echo 3 > /proc/sys/vm/drop_caches");
  const int M = 1024*1024*64;
  int* buff = new int[M];
  for (int i = 0; i < M; i++) buff[i] = rand()%1001;
  delete buff;
}


struct Request {
  int64_t address;
  bool write;
};

class RequestGenerator {
 public:
  RequestGenerator() = delete;
  RequestGenerator(int client_id, float write_ratio, int64_t store_size, 
                    std::string alignment, std::string key_distribution): 
                    generator_(std::random_device{}()), uniform_dist_(0.0, 1.0), exponential_dist_(3.0) {
    alignment_ = alignment;
    write_ratio_ = write_ratio;
    store_size_ = store_size*1024*1024;
    key_distribution_ = key_distribution;
  }
  bool IsWrite() {
    double x = uniform_dist_(generator_);
    return x <= write_ratio_;
  }
  int64_t GetAddress() {
    if (alignment_ == "aligned") {
      if (key_distribution_ == "uniform") {
        return int64_t((uniform_dist_(generator_) * store_size_)/4096)*4096;
      } else if (key_distribution_ == "exponential") {
        return int64_t((exponential_dist_(generator_) * store_size_)/4096)*4096;
      }
    } else if (alignment_ == "unaligned") {
      if (key_distribution_ == "uniform") {
        return int64_t((uniform_dist_(generator_) * store_size_));
      } else if (key_distribution_ == "exponential") {
        return int64_t((exponential_dist_(generator_) * store_size_));
      }
    } else {
      printf("Unknown alignment: %s\n", alignment_.c_str());
      return -1;
    }
  }
  Request GetRequest() {
    Request r;
    r.address = GetAddress();
    r.write = IsWrite();
    return r;
  }

 private:
 std::mt19937 generator_;
 std::uniform_real_distribution<double> uniform_dist_;
 std::exponential_distribution<double> exponential_dist_;
 std::string alignment_;
 double write_ratio_;
 int64_t store_size_;
 std::string key_distribution_;
};


void RunClientWorkload(int client_id, std::string server1_address, std::string server2_address, int max_retry_count, double& avg_time_us) {
  // srand(client_id);
  printf("Client: %d\n", client_id);
  std::unique_ptr<BlobClient> client(new BlobClient(server1_address, server2_address, max_retry_count));
  client->connect();
  printf("Client: %d\n Connected\n", client_id);
  fflush(stdout);
  RequestGenerator request_generator(client_id, 
                                     absl::GetFlag(FLAGS_write_ratio),
                                     absl::GetFlag(FLAGS_store_size),
                                     absl::GetFlag(FLAGS_alignment),
                                     absl::GetFlag(FLAGS_key_distribution));
  double write_time_us = 0;
  int writes = 0;
  double read_time_us = 0;
  double total_time_us = 0;
  int reads = 0;
  int num_requests = absl::GetFlag(FLAGS_requests_per_client);
  std::string write_data;
  for (int i = 0; i < 4096; i++) write_data.push_back('A' + rand()%26);
  for(int ii = 0; ii < num_requests; ++ii) {
    auto request = request_generator.GetRequest();
    if (request.write) {
      // printf("Client %d: Write %lld\n", client_id, request.address);
      auto start = std::chrono::high_resolution_clock::now();
      int res = client->write(request.address, write_data);
      auto end = std::chrono::high_resolution_clock::now();
      auto diff = end - start;
      write_time_us += diff.count() / 1000; // convert to us
      writes++;
      if (res < 0) {
        // System is always available so not being able to write
        // is a correctness issue.
        printf("Client %d: Write failed %d\n", client_id, res);
        fflush(stdout);
        exit(1);
      }
    } else {
      // printf("Client %d: Read %lld\n", client_id, request.address);
      std::string read_data;
      auto start = std::chrono::high_resolution_clock::now();
      client->read(request.address, read_data);
      auto end = std::chrono::high_resolution_clock::now();
      auto diff = end - start;
      read_time_us += diff.count() / 1000; // convert to us
      reads++;
      // printf("Client %d: Read %d\n", client_id, request.address);
    }
  }
  total_time_us = (read_time_us + write_time_us) / (reads + writes);
  read_time_us = read_time_us / reads;
  write_time_us = write_time_us / writes;
  printf("Client %d: Total requests: %d, Read Requests: %d, Write Requests: %d\n", client_id, reads+writes, reads, writes);
  printf("Client %d: Avg. time %.2f\n", client_id, total_time_us);
  printf("Client %d: Avg. Read time %.2f\n", client_id, read_time_us);
  printf("Client %d: Avg. Write time %.2f\n", client_id, write_time_us);
  avg_time_us = total_time_us;
}

int main(int argc, char* argv[]) {
  // Initialize the client.
  // absl::ParseCommandLine(argc, argv);
  srand(time(NULL));
  int num_clients = absl::GetFlag(FLAGS_num_clients);
  clear_caches();
  std::cout << "Cleared caches\n";
  std::vector<std::thread> client_threads;
  Utils utils;
  std::string conf_file = absl::GetFlag(FLAGS_config_file);
  std::cout << "Using config file: " << conf_file << std::endl;
  if (utils.parse_config_file(conf_file) != 0) {
    printf("Failed to parse config file\n");
    return -1;
  }
  std::string server1_address = utils.config["server1_address"], server2_address = utils.config["server2_address"];
  int max_retry_count = atoi(utils.config["max_retry_count"].c_str());
  std::vector<double> avg_time_us(num_clients);
  for (int ii = 0; ii < num_clients; ii++) {
    client_threads.push_back(std::thread([&]() {
      RunClientWorkload(ii, server1_address, server2_address, max_retry_count, avg_time_us[ii]);
    }));
  }
  for (auto& t: client_threads) {
    t.join();
  }
  double total_time_us = 0;
  int num_requests = absl::GetFlag(FLAGS_requests_per_client);
  for (int i = 0; i < num_clients; i++) {
    total_time_us = std::max(avg_time_us[i]*num_requests, total_time_us);
  }
  double throughput = (num_clients * num_requests * 1e6) / total_time_us; // requests per second
  printf("Total time: %.2f us\n", total_time_us);
  printf("Throughput: %.2f requests/s\n", throughput);
  return 0;
}