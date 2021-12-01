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

#include "client_thread.h"
#include "message.h"
#include "shared.h"

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<int> threads_ids {0};

class ClientOP {
  static constexpr auto max_n_operations = 100ULL;
  // NOLINTNEXTLINE (cert-err58-cpp)
  static inline std::string const random_string =
      "llllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll"
      "llllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll"
      "llllllllllllllllllllllll";
  std::atomic<size_t> global_number {0ULL};
  std::atomic<size_t> number_of_iterations {0ULL};
  // FIXME I am pretty sure this is not necessary
  std::atomic<size_t> number_of_requests {1ULL};

  void get_operation_put(sockets::client_msg::OperationData * operation_data) {
    operation_data->set_key(1);
    global_number.fetch_add(1, std::memory_order_relaxed);
    operation_data->set_type(sockets::client_msg::PUT);
  }

  void get_operation_get(sockets::client_msg::OperationData * operation_data) {
    operation_data->set_key(1);
    global_number.fetch_sub(1, std::memory_order_relaxed);
    operation_data->set_type(sockets::client_msg::GET);
  }

  void get_operation_txn_start(
      sockets::client_msg::OperationData * operation_data) {
    operation_data->set_key(1);
    global_number.fetch_sub(1, std::memory_order_relaxed);
    operation_data->set_type(sockets::client_msg::PUT);
  }

  auto get_number_of_requests() -> size_t {
    return 1;
    // FIXME probably overly pessimistic
    // FIXME we could simplify it by doing fetch_add and modulo
    auto res = number_of_requests.load(std::memory_order_relaxed);
    while (!number_of_requests.compare_exchange_weak(
        res,
        res < max_n_operations ? res + 1 : 0ULL,
        std::memory_order_acq_rel,
        std::memory_order_relaxed)) {}
    return res;
  }

public:
  auto get_operation() -> std::tuple<size_t, std::unique_ptr<char[]>, int> {
    auto i = number_of_iterations.fetch_add(1ULL, std::memory_order_relaxed);
    auto j = get_number_of_requests();

    // We could move the switch into the loop and make the code somewhat
    // simpler, however we would rely on the compiler to optimize the code
    auto operation_func = [i] {
      switch (i % 3) {
        case 0:
          return &ClientOP::get_operation_put;
        case 1:
          return &ClientOP::get_operation_get;
        case 2:
          return &ClientOP::get_operation_txn_start;
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
    return {msg_size + length_size_field, std::move(buf), j};
  }

  void verify() { fmt::print("[{}] all good\n", __func__); }
};

void client(ClientOP * client_op, int port, int nb_messages) {
  auto id = threads_ids.fetch_add(1);
  ClientThread c_thread(id);

  c_thread.connect_to_the_server(port, "localhost");

  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  sleep(2);

  auto expected_replies = 0;
  for (auto i = 0; i < nb_messages; ++i) {
    auto [size, buf, num] = client_op->get_operation();
    expected_replies += num;
    c_thread.sent_request(buf.get(), size);
    c_thread.recv_ack();
  }

  while (c_thread.replies != expected_replies) {
    c_thread.recv_ack();
  }

  client_op->verify();
}

auto main(int args, char * argv[]) -> int {
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  hostip = gethostbyname("localhost");
  constexpr auto n_expected_args = 5;
  if (args < n_expected_args) {
    fmt::print(
        stderr,
        "usage: ./client <nb_threads> <hostname> <port> <nb_messages>\n");
    return -1;
  }

  auto nb_clients = std::stoull(argv[1]);
  auto port = std::stoull(argv[3]);
  auto nb_messages = std::stoull(argv[4]);

  // creating the client threads
  std::vector<std::thread> threads;
  ClientOP client_op;

  for (size_t i = 0; i < nb_clients; i++) {
    auto id = std::make_unique<int>(i);
    threads.emplace_back(client, &client_op, port, nb_messages);
  }

  for (auto & thread : threads) {
    thread.join();
  }

  fmt::print("** all threads joined **\n");
}
