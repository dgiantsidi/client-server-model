#include <cstdint>
#include <optional>
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
    exit(1);
  }

  sockets::client_msg msg;
  auto payload_sz = bytecount - 4;
  std::string tmp(buffer.get() + 4, payload_sz);
  msg.ParseFromString(tmp);

  fmt::print("done here .. {}\n", msg.ops(0).port());

  int sockfd = -1;
  int port = msg.ops(0).port();
  if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
    fmt::print("socket\n");
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    exit(1);
  }

  // connector.s address information
  sockaddr_in their_addr {};
  their_addr.sin_family = AF_INET;
  their_addr.sin_port = htons(port);
  their_addr.sin_addr = *(reinterpret_cast<in_addr *>(he->h_addr));
  memset(&(their_addr.sin_zero), 0, sizeof(their_addr.sin_zero));

  if (connect(sockfd,
              reinterpret_cast<sockaddr *>(&their_addr),
              sizeof(struct sockaddr))
      == -1) {
    fmt::print("connect %d\n", errno);
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
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

auto ServerThread::incomming_requests() -> int {
  constexpr auto timeout_s = 10ULL;
  timeval timeout {};
  timeout.tv_sec = timeout_s;
  timeout.tv_usec = 0;
  int retval = select((max_fd + 1), &rfds, nullptr, nullptr, &timeout);
  if (retval == 0) {
    fmt::print("update connections\n");
    init();
  } else if (retval < 0) {
    fmt::print("timeout\n");
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
    cv.wait(lock);
    nb_connections = listening_sockets.size();
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

auto ServerThread::destruct_message(char * msg, size_t bytes)
    -> std::optional<uint32_t> {
  if (bytes < 4) {
    return std::nullopt;
  }

  auto actual_msg_size = convert_byte_array_to_int(msg);
  return actual_msg_size;
}

void ServerThread::post_replies() {
  for (auto & buf : queue_with_replies) {
    // TODO: secure_sent()
    auto it =
        std::find(queue_with_replies.begin(), queue_with_replies.end(), buf);
    auto ptr = std::move(std::get<1>(*it));
    auto msg_size = convert_byte_array_to_int(ptr.get());  // TODO
    auto repfd = communication_pairs[std::get<0>(*it)];
    secure_send(repfd, ptr.get(), msg_size + length_size_field);
  }
  queue_with_replies.clear();
}

auto ServerThread::read_n(int fd, char * buffer, size_t n) -> size_t {
  size_t bytes_read = 0;
  while (bytes_read < n) {
    auto bytes_left = n - bytes_read;
    auto bytes_read_now = recv(fd, buffer + bytes_read, bytes_left, 0);
    // negative return_val means that there are no more data (fine for non
    // blocking socket)
    if (bytes_read_now == 0) {
      return bytes_read_now;
    } else if (bytes_read_now > 0) {
      bytes_read += bytes_read_now;
    }
  }
  return bytes_read;
}

auto ServerThread::secure_recv(int fd)
    -> std::pair<size_t, std::unique_ptr<char[]>> {
  char dlen[4];
  if (read_n(fd, dlen, length_size_field) != length_size_field) {
    return {0, nullptr};
  }

  auto actual_msg_size_opt = destruct_message(dlen, length_size_field);
  if (!actual_msg_size_opt) {
    return {0, nullptr};
  }
  auto actual_msg_size = *actual_msg_size_opt;
  auto buf = std::make_unique<char[]>(static_cast<size_t>(actual_msg_size) + 1);
  buf[actual_msg_size] = '\0';
  if (read_n(fd, buf.get(), actual_msg_size) != actual_msg_size) {
    return {0, nullptr};
  }

  return {actual_msg_size, std::move(buf)};
}

auto ServerThread::process_req(int fd, size_t sz, char * buf) const -> void {
  sockets::client_msg msg;
  auto payload_sz = sz - 4;
  std::string tmp(buf + 4, payload_sz);
  msg.ParseFromString(tmp);
  for (auto i = 0; i < msg.ops_size(); ++i) {
    auto const & op = msg.ops(i);
    callbacks[op.type()](op, fd);
  }
}

void ServerThread::enqueue_reply(int fd, std::unique_ptr<char[]> rep) {
  queue_with_replies.push_back({fd, std::move(rep)});
}
