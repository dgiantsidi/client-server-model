#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <cxxopts.hpp>
#include <fcntl.h>
#include <fmt/format.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "kv_store.h"
#include "server_thread.h"

constexpr std::string_view usage = "usage: ./server <nb_server_threads> <port>";

// how many pending connections the queue will hold?
constexpr int backlog = 1024;

auto construct_reply(int op_id, int success, int txn_id, std::string_view val)
    -> std::unique_ptr<char[]> {
  server::server_response::reply rep;
  rep.set_op_id(op_id);
  rep.set_success(success);
  rep.set_txn_id(txn_id);
  rep.set_value(val.data(), val.size());
  // fmt::print("{} value={}\n", __func__, rep.value());

  std::string msg_str;
  rep.SerializeToString(&msg_str);

  auto msg_size = msg_str.size();
  auto buf = std::make_unique<char[]>(msg_size + length_size_field);
  construct_message(buf.get(), msg_str.c_str(), msg_size);
  return buf;  // copy-elision
}

void process_put(KvStore & db,
                 ServerThread * args,
                 sockets::client_msg::OperationData const & op,
                 int fd) {
  // fmt::print("{} key={}, value={}\n", __func__, op.key(), op.value());
  auto success = db.put(op.key(), op.value());

  auto rep_ptr = construct_reply(
      op.op_id(), success, -1 /* not a txn */, "" /* empty val */);
  args->enqueue_reply(fd, std::move(rep_ptr));
}

void process_get(KvStore const & db,
                 ServerThread * args,
                 sockets::client_msg::OperationData const & op,
                 int fd) {
  auto ret_val = db.get(op.key());

  auto rep_ptr = [&ret_val, &op]() {
    if (!ret_val) {
      fmt::print("Key: {} not found\n", op.key());
      return construct_reply(op.op_id(), 0, -1, "");
    }
    //    fmt::print("{} key={}, value={}\n", __func__, op.key(), *ret_val);
    return construct_reply(op.op_id(), 1, -1, *ret_val);
  }();

  args->enqueue_reply(fd, std::move(rep_ptr));
}

void process_tx(KvStore & db,
                ServerThread * args,
                sockets::client_msg::OperationData const & op,
                int fd) {
  switch (op.type()) {
    case sockets::client_msg::TXN_START: {
      fmt::print("{} tx_start w/ id={}\n", __func__, op.txn_id());
      auto success = db.tx_start(op.txn_id());
      break;
    }
    case sockets::client_msg::TXN_COMMIT: {
      fmt::print("{} tx_commit w/ id={}\n", __func__, op.txn_id());
      auto success = db.tx_commit(op.txn_id());
      break;
    }
    case sockets::client_msg::TXN_ABORT: {
      fmt::print("{} tx_abort w/ id={}\n", __func__, op.txn_id());
      auto success = db.tx_abort(op.txn_id());
      break;
    }
    case sockets::client_msg::TXN_GET: {
      fmt::print("{} tx_get w/ id={}\n", __func__, op.txn_id());
      auto ret_vla = db.tx_get(op.txn_id(), op.key());
      break;
    }
    case sockets::client_msg::TXN_PUT: {
      fmt::print(
          "{} tx_put w/ id={} key={}\n", __func__, op.txn_id(), op.key());
      auto success = db.tx_put(op.txn_id(), op.key(), op.value());
      auto rep_ptr =
          construct_reply(op.op_id(), success, op.txn_id(), "" /* empty val */);
      args->enqueue_reply(fd, std::move(rep_ptr));
      break;
    }
    default: {
      printf("EXIT\n");
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(-2);
    }
  }
}

static void processing_func(KvStore & db, ServerThread * args) {
  auto constexpr func_name = __func__;
  args->init();
  args->register_callback(
      sockets::client_msg::PUT,
      [&db, args](auto const & op, int fd) { process_put(db, args, op, fd); });
  args->register_callback(
      sockets::client_msg::GET,
      [&db, args](auto const & op, int fd) { process_get(db, args, op, fd); });
  args->register_callback(
      sockets::client_msg::TXN_START,
      [&db, args](auto const & op, int fd) { process_tx(db, args, op, fd); });
  args->register_callback(
      sockets::client_msg::TXN_GET,
      [&db, args](auto const & op, int fd) { process_tx(db, args, op, fd); });
  args->register_callback(
      sockets::client_msg::TXN_PUT,
      [&db, args](auto const & op, int fd) { process_tx(db, args, op, fd); });
  args->register_callback(
      sockets::client_msg::TXN_COMMIT,
      [&db, args](auto const & op, int fd) { process_tx(db, args, op, fd); });
  args->register_callback(
      sockets::client_msg::TXN_ABORT,
      [&db, args](auto const & op, int fd) { process_tx(db, args, op, fd); });

  for (bool should_continue = true; should_continue;) {
    auto ret = args->incomming_requests();

    should_continue = std::visit(
        overloaded {
            [&args, func_name](int n_fd) -> bool {
              if (n_fd <= 0) {
                auto error_msg = fmt::format(
                    "[{}] Got impossible return value {}. Value must be "
                    "greater 0",
                    func_name,
                    n_fd);
                throw std::runtime_error(error_msg);
              }
              args->get_new_requests();
              args->post_replies();
              return true;
            },
            [args, func_name](ServerThread::Timeout /*timeout*/) -> bool {
              if (args->should_exit.load(std::memory_order_relaxed)) {
                // FIXME: I am not sure that this is necessary here.
                std::atomic_thread_fence(std::memory_order_acquire);
                return false;
              }
              debug_print("[{}] Timeout\n", func_name);
              return true;
            },
            [](ErrNo err) -> bool {
              fmt::print("Error: {}\n", err.msg());
              return false;
            }},
        ret);
  }
}

auto main(int argc, char * argv[]) -> int {
  KvStore db;
  cxxopts::Options options("svr", "Example server for the sockets benchmark");
  options.allow_unrecognised_options().add_options()(
      "n,s_threads", "Number of threads", cxxopts::value<size_t>())(
      "s,hostname", "Hostname of the server", cxxopts::value<std::string>())(
      "p,port", "Port of the server", cxxopts::value<size_t>())(
      "o,one_run", "Run only one time", cxxopts::value<bool>())(
      "c,c_threads",
      "Number of clients",
      cxxopts::value<size_t>()->default_value("0"))("h,help", "Print help");

  auto args = options.parse(argc, argv);

  if (args.count("help")) {
    fmt::print("{}\n", options.help());
    return 0;
  }

  if (!args.count("s_threads")) {
    fmt::print(stderr,
               "The number of threads n_threads is required\n{}\n",
               options.help());
    return -1;
  }

  if (!args.count("hostname")) {
    fmt::print(stderr, "The hostname is required\n{}\n", options.help());
    return -1;
  }

  if (!args.count("port")) {
    fmt::print(stderr, "The port is required\n{}\n", options.help());
    return -1;
  }

  if (!args.count("c_threads")) {
    fmt::print(
        stderr, "The number of clients is required\n{}\n", options.help());
    return -1;
  }

  auto const nb_server_threads = args["s_threads"].as<size_t>();
  if (nb_server_threads == 0) {
    fmt::print(stderr, "{}\n", usage);
    return 1;
  }
  auto port = args["port"].as<size_t>();

  auto one_run = args["one_run"].as<bool>();
  auto total_clients = args["c_threads"].as<size_t>();

  std::vector<ServerThread> server_threads;
  server_threads.reserve(nb_server_threads);
  std::vector<std::thread> threads;
  threads.reserve(nb_server_threads);

  for (size_t i = 0; i < nb_server_threads; i++) {
    server_threads.emplace_back(i);
    threads.emplace_back(processing_func, std::ref(db), &server_threads[i]);
  }

  int ret = 1;

  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd == -1) {
    fmt::print("socket\n");
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    exit(1);
  }

  if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &ret, sizeof(int)) == -1) {
    fmt::print("setsockopt\n");
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    exit(1);
  }

  sockaddr_in my_addr {};
  my_addr.sin_family = AF_INET;  // host byte order
  my_addr.sin_port = htons(port);  // short, network byte order
  my_addr.sin_addr.s_addr = INADDR_ANY;  // automatically fill with my IP
  memset(&(my_addr.sin_zero),
         0,
         sizeof(my_addr.sin_zero));  // zero the rest of the struct

  if (bind(sockfd, reinterpret_cast<sockaddr *>(&my_addr), sizeof(sockaddr))
      == -1) {
    fmt::print("bind\n");
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    exit(1);
  }

  if (listen(sockfd, backlog) == -1) {
    fmt::print("listen\n");
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    exit(1);
  }

  uint64_t nb_clients = 0;
  for (;;) {
    socklen_t sin_size = sizeof(sockaddr_in);
    fmt::print("waiting for new connections ..\n");
    sockaddr_in their_addr {};
    if (one_run) {
      if (nb_clients == total_clients) {
        break;
      }
    }
    auto new_fd = accept4(sockfd,
                          reinterpret_cast<sockaddr *>(&their_addr),
                          &sin_size,
                          SOCK_CLOEXEC);
    if (new_fd == -1) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      fmt::print("accecpt() failed ..{}\n", std::strerror(errno));
      continue;
    }

    fmt::print("Received request from Client: {}:{}\n",
               inet_ntoa(their_addr.sin_addr),  // NOLINT(concurrency-mt-unsafe)
               port);
    {
      auto server_thread_id = nb_clients % nb_server_threads;
      fmt::print(
          "socket : {}  matched to thread: {}\n", new_fd, server_thread_id);
      fcntl(new_fd, F_SETFL, O_NONBLOCK);
      server_threads[server_thread_id].update_connections(new_fd);
      nb_clients++;
    }
  }

  std::atomic_thread_fence(std::memory_order_release);
  for (auto & t : server_threads) {
    t.should_exit.store(true, std::memory_order_relaxed);
  }

  for (auto & th : threads) {
    th.join();
  }

  fmt::print("[{}] all threads joined .. success\n", __func__);

  return 0;
}
