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
#include "kv_store.h"
#include "message.h"
#include "shared.h"
#include "workload_traces/generate_traces.h"

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<int> threads_ids {0};
int nb_clients = -1;
std::vector<::Workload::TraceCmd> traces;

class ClientOP {
  static constexpr auto max_n_operations = 100ULL;
  // NOLINTNEXTLINE (cert-err58-cpp)
  static inline std::string const random_string =
      "lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll"
      "l";
  std::atomic<size_t> global_number {0ULL};
  std::atomic<size_t> number_of_iterations {0ULL};
  // FIXME I am pretty sure this is not necessary
  std::atomic<size_t> number_of_requests {1ULL};
  // this is used for verification
  std::shared_ptr<KvStore> local_kv;

  void get_operation_put(sockets::client_msg::OperationData * operation_data,
                         std::vector<::Workload::TraceCmd>::iterator it) {
    operation_data->set_key(it->key_hash);
    global_number.fetch_add(1, std::memory_order_relaxed);
    operation_data->set_type(sockets::client_msg::PUT);
    operation_data->set_value(random_string);
    local_kv->put(it->key_hash, random_string);
  }

  void get_operation_get(sockets::client_msg::OperationData * operation_data,
                         std::vector<::Workload::TraceCmd>::iterator it) {
    operation_data->set_key(it->key_hash);
    global_number.fetch_sub(1, std::memory_order_relaxed);
    operation_data->set_type(sockets::client_msg::GET);
  }

  void get_operation_txn_start(
      sockets::client_msg::OperationData * operation_data,
      std::vector<::Workload::TraceCmd>::iterator it) {
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
  ClientOP() { local_kv = KvStore::init(); }

  auto get_key(uint32_t key) -> std::tuple<size_t, std::unique_ptr<char[]>> {
    sockets::client_msg msg;

    auto * operation_data = msg.add_ops();
    operation_data->set_key(key);
    operation_data->set_type(sockets::client_msg::GET);

    std::string msg_str;
    msg.SerializeToString(&msg_str);

    auto msg_size = msg_str.size();
    auto buf = std::make_unique<char[]>(msg_size + length_size_field);
    convert_int_to_byte_array(buf.get(), msg_size);
    memcpy(buf.get() + length_size_field, msg_str.data(), msg_size);
    return {msg_size + length_size_field, std::move(buf)};
  }
  auto get_operation(std::vector<::Workload::TraceCmd>::iterator & it)
      -> std::tuple<size_t, std::unique_ptr<char[]>, int> {
#if 0
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
#endif
    auto j = 1;
    auto operation_func = [&it] {
      //	fmt::print("[{}]: {} {}\n", __func__, it->op, it->key_hash);
      if (it->op == Workload::TraceCmd::put) {
        return &ClientOP::get_operation_put;
      }
      if (it->op == Workload::TraceCmd::get) {
        return &ClientOP::get_operation_get;
      }
    }();

    sockets::client_msg msg;

    for (auto k = 0ULL; k < j; ++k) {
      auto * operation_data = msg.add_ops();
      (this->*operation_func)(operation_data, it);
    }

    it++;
    std::string msg_str;
    msg.SerializeToString(&msg_str);

    auto msg_size = msg_str.size();
    auto buf = std::make_unique<char[]>(msg_size + length_size_field);
    convert_int_to_byte_array(buf.get(), msg_size);
    memcpy(buf.get() + length_size_field, msg_str.data(), msg_size);
    return {msg_size + length_size_field, std::move(buf), j};
  }

  void verify(int key, const char * ret_val, size_t bytecount) {
    auto expected_val = local_kv->get(key);
    if (::memcmp(ret_val, expected_val->data(), bytecount) != 0) {
      fmt::print("[{}] ERROR on key={} {} != {}\n",
                 __func__,
                 key,
                 ret_val,
                 expected_val->data());
    } else {
      // fmt::print("[{}] all good\n", __func__);
    }
  }
};

void client(ClientOP * client_op, int port, int nb_messages) {
  auto id = threads_ids.fetch_add(1);
  ClientThread c_thread(id);

  c_thread.connect_to_the_server(port, "localhost");

  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  sleep(2);

  auto expected_replies = 0;
  auto step = nb_messages / nb_clients;
  auto it = traces.begin() + step * id;
  fmt::print("{} - {}\n", step * id, step * id + step);
  for (auto i = 0; i < step; ++i) {
    auto [size, buf, num] = client_op->get_operation(it);
    expected_replies += num;
    c_thread.sent_request(buf.get(), size);
    c_thread.recv_ack();
  }

  while (c_thread.replies != expected_replies) {
    c_thread.recv_ack();
  }

  // verify
  it = traces.begin() + step * id;
  for (auto i = 0; i < step; i++) {
    auto [size, buf] = client_op->get_key(it->key_hash);
    c_thread.sent_request(buf.get(), size);
    auto [bytecount, result] = c_thread.recv_ack();

    server::server_response::reply msg;
    auto payload_sz = bytecount;
    std::string tmp(result.get(), payload_sz);
    msg.ParseFromString(tmp);
    // fmt::print("{} recv={} and {}\n", __func__, msg.value().size(),
    // msg.value());

    client_op->verify(it->key_hash, msg.value().c_str(), msg.value().size());
    // fmt::print("{} \n", result.get());
  }
}

auto main(int args, char * argv[]) -> int {
  // initialize workload
  std::string file =
      "/home/dimitra/workspace/client-server-model/source/workload_traces/"
      "12K_traces.txt";
  // reserve space for the vector --
  traces.reserve(1000000);
  traces = ::Workload::trace_init(file, gets_per_mille);

  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  hostip = gethostbyname("localhost");
  constexpr auto n_expected_args = 5;
  if (args < n_expected_args) {
    fmt::print(
        stderr,
        "usage: ./client <nb_threads> <hostname> <port> <nb_messages>\n");
    return -1;
  }

  nb_clients = std::stoull(argv[1]);
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
