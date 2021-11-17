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

#include "server_thread.h"

constexpr std::string_view usage = "usage: ./server <nb_server_threads> <port>";

// how many pending connections the queue will hold?
constexpr int backlog = 1024;

static void processing_func(ServerThread * args) {
  args->init();
  for (;;) {
    int ret = args->incomming_requests();

    if (ret > 0) {
      // new req
      // pass func1 as callback that will do the
      // actual req processing
      args->get_new_requests([](auto /*size*/, auto /*buffer*/) {});
      continue;
    }
  }
}

auto main(int args, char * argv[]) -> int {
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
    threads.emplace_back(processing_func, &server_threads[i]);
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
