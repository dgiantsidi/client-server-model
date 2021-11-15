#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "server_thread.h"

int nb_server_threads = 1;
int port = 1025;
constexpr int backlog =
    1024;  // how many pending connections the queue will hold

static void processing_func(std::shared_ptr<server_thread> args) {
  args->init();
  while (1) {
    int ret = args->incomming_requests();

    if (ret > 0) {
      // new req
      // pass func1 as callback that will do the
      // actual req processing
      args->get_new_requests(func1);
      continue;
    }
  }
}

int main(int args, char * argv[]) {
  if (args < 5) {
    std::cerr << "usage: ./server <nb_server_threads> <port>\n";
    return 1;
  }

  nb_server_threads = std::atoi(argv[1]);
  port = std::atoi(argv[2]);

  // That type looks wrong, a vector of shared_ptrs?????
  std::vector<std::shared_ptr<server_thread>> server_threads;
  server_threads.reserve(nb_server_threads);
  std::vector<std::thread> threads;
  threads.reserve(nb_server_threads);

  for (size_t i = 0; i < nb_server_threads; i++) {
    auto ptr = std::make_shared<server_thread>(i);
    server_threads.push_back(ptr);
    threads.emplace_back(std::thread(processing_func, server_threads[i]));
  }

  /* listen on sock_fd, new connection on new_fd */
  int sockfd, new_fd;

  /* my address information */
  sockaddr_in my_addr;

  /* connector.s address information */
  sockaddr_in their_addr;
  socklen_t sin_size;

  int ret = 1;

  if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
    perror("socket");
    exit(1);
  }

  if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &ret, sizeof(int)) == -1) {
    perror("setsockopt");
    exit(1);
  }

  my_addr.sin_family = AF_INET;  // host byte order
  my_addr.sin_port = htons(port);  // short, network byte order
  my_addr.sin_addr.s_addr = INADDR_ANY;  // automatically fill with my IP
  memset(&(my_addr.sin_zero), 0, 8);  // zero the rest of the struct

  if (bind(sockfd, (struct sockaddr *)&my_addr, sizeof(struct sockaddr))
      == -1) {
    perror("bind");
    exit(1);
  }

  if (listen(sockfd, backlog) == -1) {
    perror("listen");
    exit(1);
  }

  uint64_t nb_clients = 0;
  while (1) {
    sin_size = sizeof(struct sockaddr_in);
    std::cout << "waiting for new connections ..\n";
    if ((new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size))
        == -1) {
      std::cerr << "accept() failed .. " << std::strerror(errno) << "\n";
      continue;
    }

    printf("Received request from Client: %s:%d\n",
           inet_ntoa(their_addr.sin_addr),
           port);
    {
      auto server_thread_id = nb_clients % nb_server_threads;
      std::cout << "socket : " << new_fd
                << " matched to thread: " << server_thread_id << "\n";
      fcntl(new_fd, F_SETFL, O_NONBLOCK);
      server_threads[server_thread_id]->update_connections(new_fd);
      nb_clients++;
    }
  }

  return 0;
}
