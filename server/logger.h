#ifndef LOGGER_H
#define LOGGER_H

#include <string>
#include <fstream>
#include <iostream>
#include <vector>

#include "protos/blobstore.grpc.pb.h"

using blobstore::LogEntry;

class Logger {
private:
  std::string log_file_path_;
  std::ofstream ofs;

public:
  Logger(std::string log_file_path);

  // clear log file
  void clear_logs();

  // add a log entry to the log file
  int add_entry(int64_t txid, int64_t address1, int64_t address2, int64_t status);

  // read the entire log file
  std::vector<LogEntry> read_logs();

  // merge log entries from backup on primary and return the merged log entries
  std::vector<LogEntry> merge_logs(std::vector<LogEntry>& backup_logs);
  int refresh_logs(std::vector<LogEntry>& fresh_logs);
};

#endif