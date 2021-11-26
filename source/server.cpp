#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <fmt/format.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "kv_store.h"
#include "server_thread.h"

constexpr std::string_view usage = "usage: ./server <nb_server_threads> <port>";

// how many pending connections the queue will hold?
constexpr int backlog = 1024;

std::unique_ptr<char[]> construct_reply(int op_id,
                                        int success,
                                        int txn_id,
                                        std::string && val) {
  server::server_response::reply rep;
  rep.set_op_id(op_id);
  rep.set_success(success);
  rep.set_txn_id(txn_id);
  rep.set_value(val);

  std::string msg_str;
  rep.SerializeToString(&msg_str);

  auto msg_size = msg_str.size();
  auto buf = std::make_unique<char[]>(msg_size + length_size_field);
  convert_int_to_byte_array(buf.get(), msg_size);
  memcpy(buf.get() + length_size_field, msg_str.data(), msg_size);
  return buf;  // copy-elision
}

void process_put(KvStore & db,
                 ServerThread * args,
                 sockets::client_msg::OperationData const & op,
                 int fd) {
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
  if (!ret_val) {
    fmt::print("Key: {} not found\n", op.key());
  }

  auto rep_ptr = construct_reply(op.op_id(),
                                 ((ret_val) ? 1 : 0),
                                 -1 /* not a txn */,
                                 std::string(*ret_val));

  args->enqueue_reply(fd, std::move(rep_ptr));
  auto val = *ret_val;
  (void)val;
}

void process_txn(const sockets::client_msg::OperationData & op, int fd) {
  switch (op.type()) {
    case sockets::client_msg::TXN_START: {
      break;
    }
    case sockets::client_msg::TXN_COMMIT: {
      break;
    }
    case sockets::client_msg::TXN_ABORT: {
      break;
    }
    case sockets::client_msg::TXN_GET: {
      break;
    }
    case sockets::client_msg::TXN_PUT: {
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
  args->init();
  args->register_callback(sockets::client_msg::TXN_START, process_txn);
  args->register_callback(
      sockets::client_msg::PUT,
      [&db, args](auto const & op, int fd) { process_put(db, args, op, fd); });
  args->register_callback(
      sockets::client_msg::GET,
      [&db, args](auto const & op, int fd) { process_get(db, args, op, fd); });
  args->register_callback(sockets::client_msg::TXN_PUT, process_txn);
  args->register_callback(sockets::client_msg::TXN_GET, process_txn);
  args->register_callback(sockets::client_msg::TXN_COMMIT, process_txn);
  args->register_callback(sockets::client_msg::TXN_ABORT, process_txn);
  for (;;) {
    int ret = args->incomming_requests();

    if (ret > 0) {
      // new req
      // pass func1 as callback that will do the
      // actual req processing
      args->get_new_requests();
      args->post_replies();
      continue;
    }
  }
}

auto main(int args, char * argv[]) -> int {
  KvStore db;
  constexpr auto n_expected_args = 3;
  if (args < n_expected_args) {
    fmt::print(stderr, "{}\n", usage);
    return 1;
  }

  auto const nb_server_threads = std::stoull(argv[1]);
  if (nb_server_threads == 0) {
    fmt::print(stderr, "{}\n", usage);
    return 1;
  }
  auto port = std::stoull(argv[2]);

  std::vector<ServerThread> server_threads;
  server_threads.reserve(nb_server_threads);
  std::vector<std::thread> threads;
  threads.reserve(nb_server_threads);

  for (size_t i = 0; i < nb_server_threads; i++) {
    server_threads.emplace_back(i);
    threads.emplace_back(processing_func, std::ref(db), &server_threads[i]);
  }

  /* listen on sock_fd, new connection on new_fd */

  /* my address information */

  /* connector.s address information */

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

  return 0;
}
