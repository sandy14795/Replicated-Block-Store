#ifndef UTILS_H
#define UTILS_H

#include <unordered_map>
#include <fstream>
#define MAX_ADDRESS_LENGTH 100000


// create enums for crash types
enum CrashType {
  NO_CRASH,                           // Default value
  BACKUP_CRASH_AFTER_PRIMARY_PREPARE, // Write crash
  PRIMARY_CRASH_AFTER_LOCAL_PREPARE,  // Write crash
  PRIMARY_CRASH_AFTER_LOCAL_COMMIT,   // Write crash
  BACKUP_CRASH_AFTER_PRIMARY_COMMIT,  // Write crash
  PRIMARY_CRASH_BEFORE_READ           // Read crash
};


class Utils {
public:
  std::unordered_map<std::string, std::string> config;

  static CrashType get_crash_type(const int64_t addr) {
    if(addr > 0) {
      return CrashType::NO_CRASH;
    }
    int crash_type = abs(addr) / MAX_ADDRESS_LENGTH;
    return static_cast<CrashType>(crash_type);
  }

  static int64_t get_address(const int64_t addr) {
    if(addr > 0) {
      return addr;
    }
    int64_t address = abs(addr) % MAX_ADDRESS_LENGTH;
    return address;
  }

  static std::string crash_type_to_string(CrashType crash_type) {
    switch(crash_type) {
      case NO_CRASH:
        return "NO_CRASH";
      case BACKUP_CRASH_AFTER_PRIMARY_PREPARE:
        return "BACKUP_CRASH_AFTER_PRIMARY_PREPARE";
      case PRIMARY_CRASH_AFTER_LOCAL_PREPARE:
        return "PRIMARY_CRASH_AFTER_LOCAL_PREPARE";
      case PRIMARY_CRASH_AFTER_LOCAL_COMMIT:
        return "PRIMARY_CRASH_AFTER_LOCAL_COMMIT";
      case BACKUP_CRASH_AFTER_PRIMARY_COMMIT:
        return "BACKUP_CRASH_AFTER_PRIMARY_COMMIT";
      case PRIMARY_CRASH_BEFORE_READ:
        return "PRIMARY_CRASH_BEFORE_READ";
      default:
        return "UNKNOWN";
    }
  }

  // Parse config file
  int parse_config_file(const std::string &config_file) {
      std::ifstream file(config_file, std::ios::in);
      std::string line, key, value;

      printf("Parsing config file: %s\n", config_file.c_str());
      if (file.is_open()) {
        while (getline(file, line)) {
          if (line[0] == '#' || line.length() == 0) {
            continue;
          }

          int pos = line.find("=");
          key = line.substr(0, pos);
          value = line.substr(pos + 1);

          config[key] = value;
        }
        file.close();
      } else {
        std::cout << "Unable to open file " << config_file << std::endl;
        return -1;
      }

      return 0;
    }
};

#endif // UTILS_H