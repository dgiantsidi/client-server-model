#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "client_message.pb.h"
#include "client_thread.h"
#include "shared.h"

// NOLINTNEXTLINE (cert-err58-cpp)
std::string const random_string =
    "llllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll"
    "llllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll"
    "llllllllllllllllllll";

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<uint32_t> global_number {0};

// TODO Remove code duplications
auto get_operation() -> std::pair<size_t, std::unique_ptr<char[]>> {
  constexpr auto length_size_field = sizeof(uint32_t);
  // FIXME that seems very expensive having two variables with mutexes attached
  // to them...
  static auto i = 0ULL;
  static auto j = 1ULL;
  constexpr auto max_n_operation = 100ULL;
  if (j > max_n_operation) {
    j = 0;
  }

  sockets::client_msg msg;
  if (i % 3 == 0) {
    i++;
    for (auto k = 0ULL; k < j; k++) {
      sockets::client_msg::OperationData * op = msg.add_ops();
      op->set_argument(1);
      global_number.fetch_add(1);
      op->set_type(sockets::client_msg::ADD);
    }
    j++;

    std::string msg_str;
    msg.SerializeToString(&msg_str);
    char number[length_size_field];
    size_t sz = msg_str.size();
    convert_int_to_byte_array(number, sz);
    std::unique_ptr<char[]> buf =
        std::make_unique<char[]>(sz + length_size_field);
    ::memcpy(buf.get(), number, length_size_field);

    ::memcpy(buf.get() + length_size_field, msg_str.c_str(), sz);
    return {sz + length_size_field, std::move(buf)};
  }
  if (i % 3 == 1) {
    i++;
    for (auto k = 0ULL; k < j; k++) {
      sockets::client_msg::OperationData * op = msg.add_ops();
      op->set_argument(1);
      global_number.fetch_sub(1);
      op->set_type(sockets::client_msg::SUB);
    }
    j++;

    std::string msg_str;
    msg.SerializeToString(&msg_str);

    char number[length_size_field];
    size_t sz = msg_str.size();
    convert_int_to_byte_array(number, sz);
    std::unique_ptr<char[]> buf =
        std::make_unique<char[]>(sz + length_size_field);
    ::memcpy(buf.get(), number, length_size_field);
    ::memcpy(buf.get() + length_size_field, msg_str.c_str(), sz);
    return {sz + length_size_field, std::move(buf)};
  }

  i++;
  for (auto k = 0ULL; k < j; k++) {
    sockets::client_msg::OperationData * op = msg.add_ops();
    op->set_random_data(random_string);
    op->set_type(sockets::client_msg::RANDOM_DATA);
  }
  j++;

  std::string msg_str;
  msg.SerializeToString(&msg_str);

  char number[length_size_field];
  size_t sz = msg_str.size();
  convert_int_to_byte_array(number, sz);
  std::unique_ptr<char[]> buf =
      std::make_unique<char[]>(sz + length_size_field);
  ::memcpy(buf.get(), number, length_size_field);
  ::memcpy(buf.get() + length_size_field, msg_str.c_str(), sz);
  return {sz + length_size_field, std::move(buf)};
}

void client(int port, int nb_messages) {
  ClientThread c_thread {};

  c_thread.connect_to_the_server(port, "localhost");
  for (auto iterations = nb_messages; iterations > 0; --iterations) {
    auto [size, buf] = get_operation();
    c_thread.sent_request(buf.get(), size);
  }
}

auto main(int args, char * argv[]) -> int {
  constexpr auto n_expected_args = 5;
  if (args < n_expected_args) {
    std::cerr
        << "usage: ./client <nb_threads> <hostname> <port> <nb_messages>\n";
    return -1;
  }

  auto nb_clients = std::stoull(argv[1]);
  auto port = std::stoull(argv[3]);
  auto nb_messages = std::stoull(argv[4]);

  // creating the client threads
  std::vector<std::thread> threads;

  for (size_t i = 0; i < nb_clients; i++) {
    threads.emplace_back(client, port, nb_messages);
  }

  for (auto & thread : threads) {
    thread.join();
  }

  std::cout << "** all threads joined **\n";
}
