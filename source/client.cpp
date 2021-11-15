#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "client_message.pb.h"
#include "client_thread.h"
#include "shared.h"

int nb_clients = -1;
int port = -1;
int nb_messages = -1;

uint64_t last_result = 0;

std::string _string =
    "llllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll"
    "llllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll"
    "llllllllllllllllllll";
std::atomic<uint32_t> global_number {0};

std::unique_ptr<char[]> get_operation(size_t & size) {
  static int i = 0;
  static int j = 1;
  if (j > 100) {
    j = 0;
  }

  sockets::client_msg _msg;
  if (i % 3 == 0) {
    i++;
    for (int k = 0; k < j; k++) {
      sockets::client_msg::OperationData * op = _msg.add_ops();
      op->set_argument(1);
      global_number.fetch_add(1);
      op->set_type(sockets::client_msg::ADD);
    }
    j++;

    std::string msg_str;
    _msg.SerializeToString(&msg_str);
    char number[4];
    size_t sz = msg_str.size();
    convertIntToByteArray(number, sz);
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(sz + 4);
    ::memcpy(buf.get(), number, 4);

    ::memcpy(buf.get() + 4, msg_str.c_str(), sz);
    size = sz + 4;

    return std::move(buf);

  } else if (i % 3 == 1) {
    i++;
    for (int k = 0; k < j; k++) {
      sockets::client_msg::OperationData * op = _msg.add_ops();
      op->set_argument(1);
      global_number.fetch_sub(1);
      op->set_type(sockets::client_msg::SUB);
    }
    j++;

    std::string msg_str;
    _msg.SerializeToString(&msg_str);

    char number[4];
    size_t sz = msg_str.size();
    convertIntToByteArray(number, sz);
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(sz + 4);
    ::memcpy(buf.get(), number, 4);
    ::memcpy(buf.get() + 4, msg_str.c_str(), sz);
    size = sz + 4;
    return std::move(buf);

  } else {
    i++;
    for (int k = 0; k < j; k++) {
      sockets::client_msg::OperationData * op = _msg.add_ops();
      op->set_random_data(_string);
      op->set_type(sockets::client_msg::RANDOM_DATA);
    }
    j++;

    std::string msg_str;
    _msg.SerializeToString(&msg_str);

    char number[4];
    size_t sz = msg_str.size();
    convertIntToByteArray(number, sz);
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(sz + 4);
    ::memcpy(buf.get(), number, 4);
    ::memcpy(buf.get() + 4, msg_str.c_str(), sz);
    size = sz + 4;
    return std::move(buf);
  }
}

void client() {
  class client_thread c_thread;

  c_thread.connect_to_the_server(port, "localhost");
  uint64_t bytes_sent = 0;
  int iterations = nb_messages;
  while (iterations > 0) {
    size_t size = 0;
    std::unique_ptr<char[]> buf = get_operation(size);
    c_thread.sent_request(buf.get(), size);
    iterations--;
  }
}

int main(int args, char * argv[]) {
  if (args < 5) {
    std::cerr
        << "usage: ./client <nb_threads> <hostname> <port> <nb_messages>\n";
    exit(-1);
  }

  nb_clients = std::atoi(argv[1]);
  port = std::atoi(argv[3]);
  nb_messages = std::atoi(argv[4]);

  // creating the client threads
  std::vector<std::thread> threads;

  for (size_t i = 0; i < nb_clients; i++) {
    threads.emplace_back(std::thread(client));
  }

  for (auto & thread : threads) {
    thread.join();
  }

  std::cout << "** all threads joined **\n";
}
