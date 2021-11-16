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

class ClientOP {
  static constexpr auto max_n_operations = 100ULL;
  static constexpr auto length_size_field = sizeof(uint32_t);
  // NOLINTNEXTLINE (cert-err58-cpp)
  static inline std::string const random_string =
      "llllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll"
      "llllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll"
      "llllllllllllllllllllllll";
  // NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
  std::atomic<size_t> global_number {0ULL};
  std::atomic<size_t> number_of_iterations {0ULL};
  std::atomic<size_t> number_of_requests {1ULL};

  void get_operation_add(sockets::client_msg::OperationData * operation_data) {
    operation_data->set_argument(1);
    global_number.fetch_add(1);
    operation_data->set_type(sockets::client_msg::ADD);
  }

  void get_operation_sub(sockets::client_msg::OperationData * operation_data) {
    operation_data->set_argument(1);
    global_number.fetch_sub(1);
    operation_data->set_type(sockets::client_msg::SUB);
  }

  // NOLINTNEXTLINE (readablitiy-convert-member-functions-to-static)
  void get_operation_random(
      sockets::client_msg::OperationData * operation_data) {
    operation_data->set_random_data(random_string);
    operation_data->set_type(sockets::client_msg::RANDOM_DATA);
  }

  auto get_number_of_requests() -> size_t {
    auto res = number_of_requests.load(std::memory_order_relaxed);
    while (!number_of_requests.compare_exchange_weak(
        res,
        res < max_n_operations ? res + 1 : 0ULL,
        std::memory_order_acq_rel,
        std::memory_order_relaxed)) {}
    return res;
  }

public:
  auto get_operation() -> std::pair<size_t, std::unique_ptr<char[]>> {
    auto i = number_of_iterations.fetch_add(1ULL, std::memory_order_relaxed);
    auto j = get_number_of_requests();

    auto operation_func = [i] {
      switch (i % 3) {
        case 0:
          return &ClientOP::get_operation_add;
        case 1:
          return &ClientOP::get_operation_sub;
        case 2:
          return &ClientOP::get_operation_random;
        default:
          throw std::runtime_error("Unknown operation");
      }
    }();

    sockets::client_msg msg;

    for (auto k = 0ULL; k < j; ++k) {
      auto * operation_data = msg.add_ops();
      (this->*operation_func)(operation_data);
    }

    std::string msg_str;
    msg.SerializeToString(&msg_str);

    auto msg_size = msg_str.size();
    auto buf = std::make_unique<char[]>(msg_size + length_size_field);
    convert_int_to_byte_array(buf.get(), msg_size);
    memcpy(buf.get() + length_size_field, msg_str.data(), msg_size);
    return {msg_size + length_size_field, std::move(buf)};
  }
};

void client(ClientOP * client_op, int port, int nb_messages) {
  ClientThread c_thread {};

  c_thread.connect_to_the_server(port, "localhost");
  for (auto iterations = nb_messages; iterations > 0; --iterations) {
    auto [size, buf] = client_op->get_operation();
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
  ClientOP client_op;

  for (size_t i = 0; i < nb_clients; i++) {
    threads.emplace_back(client, &client_op, port, nb_messages);
  }

  for (auto & thread : threads) {
    thread.join();
  }

  std::cout << "** all threads joined **\n";
}
