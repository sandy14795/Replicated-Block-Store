#include "logger.h"

Logger::Logger(std::string log_file_path) : log_file_path_(log_file_path) {
    ofs.open(log_file_path_, std::ios::app | std::ios::binary);
    if(!ofs.is_open()){
        std::cout << "Logger::Logger() - Failed to open log file: " << log_file_path << std::endl;
        exit(1);
    }
}

void Logger::clear_logs() {
    // Close file if already open
    if(ofs.is_open()){
        ofs.close();
    }

    // Truncate file
    ofs.open(log_file_path_, std::ios::trunc | std::ios::binary);
    ofs.close();

    // Open file for appending log entries
    ofs.open(log_file_path_, std::ios::app | std::ios::binary);
}

int Logger::add_entry(int64_t txid, int64_t address1, int64_t address2, int64_t status){
    std::vector<int64_t> entry = {txid, address1, address2, status};
    ofs.write((char*)(entry.data()), entry.size() * sizeof(int64_t));
    ofs.flush();
    if(ofs.fail()){
        std::cout << "Logger::add_entry() - Failed to write to log file: " << log_file_path_ << std::endl;
        return -1;
    }
    #ifdef debug
    std::cout << "Wrote to log file: " << log_file_path_ << std::endl;
    #endif
    return 0;
}

std::vector<LogEntry> Logger::read_logs(){ 

    std::vector<LogEntry> logs;
    std::ifstream ifs(log_file_path_, std::ios::binary);
    if(!ifs.is_open()){
        std::cout << "Logger::read_logs() - Failed to open log file: " << log_file_path_ << std::endl;
        exit(1);
    }

    while(!ifs.eof()){
        int entry_size = 4;
        std::vector<int64_t> read_entry(entry_size);
        // Read 1 log entry
        ifs.read((char*)(read_entry.data()), read_entry.size() * sizeof(int64_t));
        
        #ifdef debug
        std::cout << "Read from log file " << log_file_path_ << " : " << read_entry[0] << " " << read_entry[1] << " " << read_entry[2] << " " << read_entry[3] << std::endl;
        #endif
        LogEntry entry;
        entry.set_txid(read_entry[0]);
        entry.set_address1(read_entry[1]);
        entry.set_address2(read_entry[2]);
        entry.set_status(read_entry[3]);

        if(!read_entry[0] == 0){ //to_be_implemented/checked again, because of eof? reading into vector giving one entry with all zeroes even if file is empty
            logs.push_back(entry);
        }
    }
    return logs;
}

std::vector<LogEntry> Logger::merge_logs(std::vector<LogEntry>& backup_logs){
    std::vector<LogEntry> self_logs = read_logs();

    std::cout << "Primary: " << self_logs.size() << " entries, Backup: " << backup_logs.size() << " entries." << std::endl;

    std::cout << "[Recovery]: (Primary) Start merging logs" << std::endl;
    if(self_logs.empty() || backup_logs.empty()){
        return self_logs;
    }
  
    int self_log_size = self_logs.size();
    int backup_log_size = backup_logs.size();
    int tentative_end_pos = std::min(self_log_size,backup_log_size)-1;
    std::cout << "[Recovery]: (Primary) tentative end pos " + std::to_string(tentative_end_pos) << std::endl;

    // If all txid monotonic? less expensive to search!
    while(tentative_end_pos >=0 && self_logs[tentative_end_pos].txid() != backup_logs[tentative_end_pos].txid()){
        tentative_end_pos -= 1;
    }
    std::cout << "[Recovery]: (Primary) Final end pos " + std::to_string(tentative_end_pos) << std::endl;
    if (tentative_end_pos >= 0){ // Check not required?
        self_logs.erase(self_logs.begin(), self_logs.begin() + tentative_end_pos + 1);
    }

    return self_logs;
}


int Logger::refresh_logs(std::vector<LogEntry>& fresh_logs){
    #ifdef debug
    std::cout << "[Recovery]: (Primary) Refreshing logs" << std::endl;
    #endif
    ofs.close();
    ofs.open(log_file_path_, std::ios::trunc | std::ios::binary); //to_be_implemented - to check another way to remove entries and start fresh - delete?
    ofs.close();
    ofs.open(log_file_path_, std::ios::app | std::ios::binary);
    for(auto entry:fresh_logs){
        add_entry(entry.txid(), entry.address1(), entry.address2(), entry.status());
    }
    #ifdef debug
    std::cout << "[Recovery]: (Primary) Refreshed logs on primary" << std::endl;
    #endif

  return 0;
}