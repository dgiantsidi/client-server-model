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
#include <cxxopts.hpp>
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
int nb_messages = 1200;
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
    auto item = it->operation[0];
    operation_data->set_key(item.key_hash);
    global_number.fetch_add(1, std::memory_order_relaxed);
    operation_data->set_type(sockets::client_msg::PUT);
    operation_data->set_value(random_string);
    local_kv->put(item.key_hash, random_string);
  }

  void get_operation_get(sockets::client_msg::OperationData * operation_data,
                         std::vector<::Workload::TraceCmd>::iterator it) {
    auto item = it->operation[0];
    operation_data->set_key(item.key_hash);
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

  auto get_tx_type(int op) {
    if (op == ::Workload::TraceCmd::txn_start) {
      return sockets::client_msg::TXN_START;
    }
    if (op == ::Workload::TraceCmd::txn_put) {
      return sockets::client_msg::TXN_PUT;
    }
    if (op == ::Workload::TraceCmd::txn_get) {
      return sockets::client_msg::TXN_GET;
    }
    if (op == ::Workload::TraceCmd::txn_get_and_execute) {
      return sockets::client_msg::TXN_GET_AND_EXECUTE;
    }
    if (op == ::Workload::TraceCmd::txn_commit) {
      return sockets::client_msg::TXN_COMMIT;
    }
    if (op == ::Workload::TraceCmd::txn_rollback) {
      return sockets::client_msg::TXN_ABORT;
    }
  }

  auto get_tx(std::vector<::Workload::TraceCmd>::iterator & it, int thread_id)
      -> std::tuple<size_t, std::unique_ptr<char[]>, int> {
    // todo::@dimitra
    static int tx_ids = 0;

    sockets::client_msg msg;
    auto op_nb = 0;
    for (auto & op : it->operation) {
      auto * operation_data = msg.add_ops();
      operation_data->set_client_id(thread_id);
      operation_data->set_txn_id(tx_ids);
      operation_data->set_op_id(op_nb++);
      operation_data->set_key(op.key_hash);
      operation_data->set_value(op.value);
      operation_data->set_type(get_tx_type(op.op));
    }

    tx_ids++;
    std::string msg_str;
    msg.SerializeToString(&msg_str);

    auto msg_size = msg_str.size();
    if (msg_size == 0) {
      fmt::print("[{}] ERROR: it->operation.size()={}\n",
                 __func__,
                 it->operation.size());
      std::cout << msg.DebugString() << "\n";
      sleep(2);
    }
    if (it != (traces.end() - 1)) {
      it++;
    } else {
      it = traces.begin()
          + (traces.size() / nb_clients) * (rand() % (nb_clients - 1));
    }

    auto buf = std::make_unique<char[]>(msg_size + length_size_field);
    convert_int_to_byte_array(buf.get(), msg_size);
    memcpy(buf.get() + length_size_field, msg_str.data(), msg_size);
    return {msg_size + length_size_field, std::move(buf), op_nb};
  }

  auto get_operation(std::vector<::Workload::TraceCmd>::iterator & it)
      -> std::tuple<size_t, std::unique_ptr<char[]>, int> {
    auto operation_func = [it] {
      //	fmt::print("[{}]: {} {}\n", __func__, it->op, it->key_hash);
      if (it->operation[0].op == Workload::TraceCmd::put) {
        return &ClientOP::get_operation_put;
      }
      if (it->operation[0].op == Workload::TraceCmd::get) {
        return &ClientOP::get_operation_get;
      }
    }();

    sockets::client_msg msg;

    auto * operation_data = msg.add_ops();
    (this->*operation_func)(operation_data, it);

    if (it != (traces.end() - 1)) {
      it++;
    } else {
      it = traces.begin()
          + (traces.size() / nb_clients) * (rand() % (nb_clients - 1));
    }

    std::string msg_str;
    msg.SerializeToString(&msg_str);

    auto msg_size = msg_str.size();
    auto buf = std::make_unique<char[]>(msg_size + length_size_field);
    convert_int_to_byte_array(buf.get(), msg_size);
    memcpy(buf.get() + length_size_field, msg_str.data(), msg_size);
    return {msg_size + length_size_field, std::move(buf), 1};
  }

  void verify(int key, const char * ret_val, size_t bytecount) {
    auto expected_val = local_kv->get(key);
    if (expected_val->data() == nullptr) {
      if (bytecount != 0)
        exit(8);
      return;
    }
    if (::memcmp(ret_val, expected_val->data(), bytecount) != 0) {
      fmt::print("[{}] ERROR on key={} {} != {}\n",
                 __func__,
                 key,
                 ret_val,
                 expected_val->data());
      exit(1);
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
  auto step = traces.size() / nb_clients;
  auto it = traces.begin() + step * id;
  fmt::print("{} {} - {}\n", step, step * id, step * id + step);
  for (auto i = 0; i < nb_messages; ++i) {
    // auto [size, buf, num] = client_op->get_operation(it);
    auto [size, buf, num] = client_op->get_tx(it, id);
    expected_replies += num;
    c_thread.sent_request(buf.get(), size);
    c_thread.recv_ack();
  }

  while (c_thread.replies != expected_replies) {
    // fmt::print("received replies={}\n", c_thread.replies);
    c_thread.recv_ack();
  }

#if 0
  // verify
  it = traces.begin() + step * id;
  for (auto i = 0; i < step; i++) {
    auto ops = it->operation;
    for (auto & item : ops) {
      auto [size, buf] = client_op->get_key(item.key_hash);
      c_thread.sent_request(buf.get(), size);
      auto [bytecount, result] = c_thread.recv_ack();

      server::server_response::reply msg;
      auto payload_sz = bytecount;
      std::string tmp(result.get(), payload_sz);
      msg.ParseFromString(tmp);
      // fmt::print("{} recv={} and {}\n", __func__, msg.value().size(),
      // msg.value());

      client_op->verify(item.key_hash, msg.value().c_str(), msg.value().size());
      // fmt::print("{} \n", result.get());
    }
  }
#endif
}

auto main(int argc, char * argv[]) -> int {
  cxxopts::Options options(argv[0], "Client for the sockets benchmark");
  options.allow_unrecognised_options().add_options()(
      "c,c_threads",
      "Number of threads the client should use",
      cxxopts::value<size_t>())(
      "s,hostname", "Hostname of the server", cxxopts::value<std::string>())(
      "p,port", "Port of the server", cxxopts::value<size_t>())(
      "m,n_messages",
      "Number of messages to send to the server",
      cxxopts::value<size_t>())(
      "t,trace", "Trace file to use", cxxopts::value<std::string>())(
      "h,help", "Print help");
  //  ("positional", "Positional argument",
  //  cxxopts::value<std::vector<std::string>>());
  // options.parse_positional({"n_threads", "hostname", "port", "n_messages",
  // "trace", "positional"});

  auto args = options.parse(argc, argv);
  if (args.count("help")) {
    fmt::print("{}\n", options.help());
    return 0;
  }

  if (!args.count("c_threads")) {
    fmt::print(stderr,
               "The number of threads n_threads is required\n{}\n",
               options.help());
    return 1;
  }

  if (!args.count("hostname")) {
    fmt::print(stderr, "The hostname is required\n{}\n", options.help());
    return 1;
  }

  if (!args.count("port")) {
    fmt::print(stderr, "The port is required\n{}\n", options.help());
    return 1;
  }

  if (!args.count("n_messages")) {
    fmt::print(
        stderr, "The number of messages is required\n{}\n", options.help());
    return 1;
  }

  if (!args.count("trace")) {
    fmt::print(stderr, "The trace file is required\n{}\n", options.help());
    return 1;
  }

  // initialize workload
  traces =
      ::Workload::trace_init(args["trace"].as<std::string>(), gets_per_mille);
  if (traces.empty()) {
    fmt::print(stderr, "The trace file is empty\n");
    return 1;
  }

  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  hostip = gethostbyname("localhost");

  nb_clients = args["c_threads"].as<size_t>();
  auto port = args["port"].as<size_t>();
  nb_messages = args["n_messages"].as<size_t>();

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
