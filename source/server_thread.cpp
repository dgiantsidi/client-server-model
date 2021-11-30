#include <cstdint>
#include <cstring>
#include <optional>
#include <variant>
#include <vector>

#include "server_thread.h"

#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

auto ServerThread::operator=(ServerThread && other) noexcept -> ServerThread & {
  fmt::print("{}\n", __PRETTY_FUNCTION__);
  std::lock_guard lock(mtx);
  std::lock_guard lock2(other.mtx);
  id = other.id;
  max_fd = other.max_fd;
  rfds = other.rfds;
  listening_sockets = std::move(other.listening_sockets);
  other.id = -1;
  other.max_fd = -1;
  return *this;
}

void ServerThread::create_communication_pair(int listening_socket) {
  auto * he = hostip;

  auto [bytecount, buffer] = secure_recv(listening_socket);
  if (bytecount == 0) {
    fmt::print("Error on {}\n", __func__);
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    exit(1);
  }

  sockets::client_msg msg;
  auto payload_sz = bytecount;
  std::string tmp(buffer.get(), payload_sz);
  msg.ParseFromString(tmp);

  fmt::print("done here .. {}\n", msg.ops(0).port());

  int sockfd = -1;
  int port = msg.ops(0).port();
  if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
    fmt::print("socket {}\n", std::strerror(errno));
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    exit(1);
  }

  // connector.s address information
  sockaddr_in their_addr {};
  their_addr.sin_family = AF_INET;
  their_addr.sin_port = htons(port);
  their_addr.sin_addr = *(reinterpret_cast<in_addr *>(he->h_addr));
  memset(&(their_addr.sin_zero), 0, sizeof(their_addr.sin_zero));

  bool successful_connection = false;
  for (size_t retry = 0; retry < number_of_connect_attempts; retry++) {
    if (connect(sockfd,
                reinterpret_cast<sockaddr *>(&their_addr),
                sizeof(struct sockaddr))
        == -1) {
      //   	fmt::print("connect {}\n", std::strerror(errno));
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      sleep(1);
    } else {
      successful_connection = true;
      break;
    }
  }
  if (!successful_connection) {
    fmt::print("[{}] could not connect to client after {} attempts ..\n",
               __func__,
               number_of_connect_attempts);
    exit(1);
  }
  fmt::print("{} {}\n", listening_socket, sockfd);
  communication_pairs.insert({listening_socket, sockfd});
}

void ServerThread::update_connections(int new_sock_fd) {
  {
    std::lock_guard lock(mtx);
    listening_sockets.push_back(new_sock_fd);
    create_communication_pair(new_sock_fd);
  }
  cv.notify_one();
}

auto ServerThread::incomming_requests() -> std::variant<int, Timeout, ErrNo> {
  constexpr auto timeout_s = 10ULL;
  timeval timeout {};
  timeout.tv_sec = timeout_s;
  timeout.tv_usec = 0;
  int retval = select((max_fd + 1), &rfds, nullptr, nullptr, &timeout);
  if (retval == 0) {
    fmt::print("update connections\n");
    init();
    return Timeout {};
  } else if (retval < 0) {
    return ErrNo();
  }
  return retval;
}

void ServerThread::cleanup_connection(int dead_connection) {
  // cleanup connection
  std::lock_guard lock(mtx);
  auto it = std::find(
      listening_sockets.begin(), listening_sockets.end(), dead_connection);
  listening_sockets.erase(it);
  fmt::print("{}: {} (socket:{}={} reqs)\n",
             __PRETTY_FUNCTION__,
             dead_connection,
             dead_connection,
             reqs_per_socket[dead_connection]);

  communication_pairs.erase(dead_connection);
  reqs_per_socket.erase(dead_connection);
}

auto ServerThread::get_new_requests() -> int {
  std::vector<int> lsockets;
  {
    std::lock_guard lock(mtx);
    lsockets = listening_sockets;  // this is weird, you create a copy but do
                                   // not delete old vector?
  }
  for (auto csock : lsockets) {
    if (FD_ISSET(csock, &rfds)) {  // NOLINT
      auto [bytecount, buffer] = secure_recv(csock);
      if (static_cast<int>(bytecount) <= 0) {
        if (static_cast<int>(bytecount) == 0) {
          cleanup_connection(csock);
          init();
        }
      } else {
        // FIXME: We expect the provider of the function to handle error cases!
        if (reqs_per_socket.find(csock) == reqs_per_socket.end()) {
          reqs_per_socket.insert({csock, 0});
        } else {
          reqs_per_socket[csock] += 1;
        }
        process_req(csock, bytecount, buffer.get());
      }
    }
  }
  // FIXME What do we actually return here???
  return 0;
}

auto ServerThread::init() -> void {
  get_new_connections();
  reset_fds();
}

void ServerThread::get_new_connections() {
  std::unique_lock<std::mutex> lock(mtx);
  auto nb_connections = listening_sockets.size();
  while (nb_connections == 0) {
    fmt::print("{}: no connections\n", __PRETTY_FUNCTION__);

    using namespace std::chrono_literals;
    cv.wait_for(lock, 1000ms);
    nb_connections = listening_sockets.size();
    if (nb_connections == 0 && should_exit.load(std::memory_order_relaxed)) {
      std::atomic_thread_fence(std::memory_order_acquire);
      debug_print("[{}]: No connection could be established...\n",
                  __PRETTY_FUNCTION__);
      return;
    }
  }
}

void ServerThread::reset_fds() {
  max_fd = -1;
  FD_ZERO(&rfds);  // NOLINT
  for (auto rfd : listening_sockets) {
    FD_SET(rfd, &rfds);  // NOLINT
    max_fd = (max_fd < rfd) ? rfd : max_fd;
  }
}

void ServerThread::post_replies() {
  for (auto & buf : queue_with_replies) {
    auto it =
        std::find(queue_with_replies.begin(), queue_with_replies.end(), buf);
    auto ptr = std::move(std::get<1>(*it));
    auto msg_size = convert_byte_array_to_int(ptr.get());  // TODO
    auto repfd = communication_pairs[std::get<0>(*it)];
    //   fmt::print("[{}] sent {} bytes\n", __func__, msg_size);
    secure_send(repfd, ptr.get(), msg_size + length_size_field);
  }
  queue_with_replies.clear();
}

auto ServerThread::process_req(int fd, size_t sz, char * buf) const -> void {
  sockets::client_msg msg;
  auto payload_sz = sz;
  std::string tmp(buf, payload_sz);
  msg.ParseFromString(tmp);
  for (auto i = 0; i < msg.ops_size(); ++i) {
    auto const & op = msg.ops(i);
    callbacks[op.type()](op, fd);
  }
}

void ServerThread::enqueue_reply(int fd, std::unique_ptr<char[]> rep) {
  queue_with_replies.emplace_back(fd, std::move(rep));
}
