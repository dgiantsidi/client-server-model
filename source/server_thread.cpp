#include <cstdint>
#include <optional>
#include <vector>

#include "server_thread.h"

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

void ServerThread::update_connections(int new_sock_fd) {
  {
    std::lock_guard lock(mtx);
    listening_sockets.push_back(new_sock_fd);
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
  fmt::print("{}: {}\n", __PRETTY_FUNCTION__, dead_connection);
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
      if (bytecount <= 0) {
        if (bytecount == 0) {
          cleanup_connection(csock);
          init();
        }
      }
      // FIXME: We expect the provider of the function to handle error cases!
      process_req(bytecount, buffer.get());
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

auto ServerThread::read_n(int fd, char * buffer, size_t n) -> size_t {
  size_t bytes_read = 0;
  while (bytes_read < n) {
    auto bytes_left = n - bytes_read;
    auto bytes_read_now = recv(fd, buffer + bytes_read, bytes_left, 0);
    if (bytes_read_now <= 0) {
      return bytes_read_now;
    }
    bytes_read += bytes_read_now;
  }
  return bytes_read;
}

auto ServerThread::secure_recv(int fd)
    -> std::pair<uint32_t, std::unique_ptr<char[]>> {
  char dlen[4];
  if (auto byte_read = read_n(fd, dlen, length_size_field);
      byte_read != length_size_field) {
    return {byte_read, nullptr};
  }

  auto actual_msg_size_opt = destruct_message(dlen, length_size_field);
  if (!actual_msg_size_opt) {
    return {-1, nullptr};
  }
  auto actual_msg_size = *actual_msg_size_opt;
  auto buf = std::make_unique<char[]>(static_cast<size_t>(actual_msg_size) + 1);
  buf[actual_msg_size] = '\0';
  if (auto byte_read = read_n(fd, buf.get(), actual_msg_size);
      byte_read != actual_msg_size) {
    return {byte_read, nullptr};
  }

  return {actual_msg_size, std::move(buf)};
}

auto ServerThread::process_req(size_t sz, char * buf) const -> void {
  sockets::client_msg msg;
  auto payload_sz = sz - 4;
  std::string tmp(buf + 4, payload_sz);
  msg.ParseFromString(tmp);
  for (auto i = 0; i < msg.ops_size(); ++i) {
    auto const & op = msg.ops(i);
    callbacks[op.op_id()](op);
  }
}