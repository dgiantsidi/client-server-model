#pragma once

#include <algorithm>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <variant>

#include <fmt/format.h>
#include <unistd.h>

#include "callback.h"
#include "message.h"
#include "shared.h"

// https://stackoverflow.com/a/57757301
namespace Details {
template<typename T, std::size_t... Is>
auto create_array(T value, std::index_sequence<Is...> /*unused*/)
    -> std::array<T, sizeof...(Is)> {
  // cast Is to void to remove the warning: unused value
  return {{(static_cast<void>(Is), value)...}};
}

inline void unimplemented(sockets::client_msg::OperationData const & msg) {
  auto const & op = msg.type();
  auto const & error_str =
      fmt::format("Unimplemented operation: {}",
                  sockets::client_msg::OperationType_Name(op));
  throw std::runtime_error(error_str);
}
}  // namespace Details

class ServerThread {
public:
  constexpr static size_t n_ops = sockets::client_msg::OperationType_ARRAYSIZE;
  using CallbackT =
      std::function<void(sockets::client_msg::OperationData const &)>;
  using CallbackArrayT = std::array<CallbackT, n_ops>;

  ServerThread() = delete;
  inline explicit ServerThread(int i)
      : id(i)
      , max_fd(-1)
      , rfds() {};

  ServerThread(ServerThread const & other) = delete;
  auto operator=(ServerThread const & other) -> ServerThread & = delete;

  inline ServerThread(ServerThread && other) noexcept
      : id(other.id)
      , max_fd(other.max_fd)
      , rfds(other.rfds)
      , listening_sockets(std::move(other.listening_sockets)) {
    fmt::print("{}\n", __PRETTY_FUNCTION__);
    other.id = -1;
    other.max_fd = -1;
  }

  inline auto operator=(ServerThread && other) noexcept -> ServerThread & {
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

  inline ~ServerThread() {
    std::lock_guard lock(mtx);
    // TODO: close all sockets
    if (id != -1) {
      close(id);
    }
  }

  inline void update_connections(int new_sock_fd) {
    {
      std::lock_guard lock(mtx);
      listening_sockets.push_back(new_sock_fd);
    }
    cv.notify_one();
  };

  inline auto incomming_requests() -> int {
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

  inline void cleanup_connection(int dead_connection) {
    // cleanup connection
    std::lock_guard lock(mtx);
    auto it = std::find(
        listening_sockets.begin(), listening_sockets.end(), dead_connection);
    listening_sockets.erase(it);
    fmt::print("{}: {}\n", __PRETTY_FUNCTION__, dead_connection);
  }

  inline auto process_req(size_t sz, char * buf) const -> void {
    sockets::client_msg msg;
    auto payload_sz = sz - 4;
    std::string tmp(buf + 4, payload_sz);
    msg.ParseFromString(tmp);
    for (auto i = 0; i < msg.ops_size(); ++i) {
      auto const & op = msg.ops(i);
      callbacks[op.op_id()](op);
    }
  }

  inline void register_callback(sockets::client_msg::OperationType op,
                                CallbackT cb) {
    callbacks[op] = std::move(cb);
  }

  inline auto get_new_requests() -> int {
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

  void init() {
    get_new_connections();
    reset_fds();
  }

private:
  int id;
  int max_fd;
  fd_set rfds;
  CallbackArrayT callbacks = Details::create_array(
      CallbackT {Details::unimplemented}, std::make_index_sequence<n_ops>());
  std::vector<int> listening_sockets;
  std::mutex mtx;
  std::condition_variable cv;

  void get_new_connections() {
    std::unique_lock<std::mutex> lock(mtx);
    auto nb_connections = listening_sockets.size();
    while (nb_connections == 0) {
      fmt::print("{}: no connections\n", __PRETTY_FUNCTION__);
      cv.wait(lock);
      nb_connections = listening_sockets.size();
    }
  }

  void reset_fds() {
    max_fd = -1;
    FD_ZERO(&rfds);  // NOLINT
    for (auto rfd : listening_sockets) {
      FD_SET(rfd, &rfds);  // NOLINT
      max_fd = (max_fd < rfd) ? rfd : max_fd;
    }
  }

  /**
   *  * It returns the actual size of msg.
   *   * Not that msg might not contain all payload data.
   *    * The function expects at least that the msg contains the first 4 bytes
   * that
   *     * indicate the actual size of the payload.
   *      */
  static auto destruct_message(char * msg, size_t bytes)
      -> std::optional<uint32_t> {
    if (bytes < 4) {
      return std::nullopt;
    }

    auto actual_msg_size = convert_byte_array_to_int(msg);

    return actual_msg_size;
  }

  static auto read_n(int fd, char * buffer, size_t n) -> size_t {
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

  static auto secure_recv(int fd)
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
    auto buf =
        std::make_unique<char[]>(static_cast<size_t>(actual_msg_size) + 1);
    buf[actual_msg_size] = '\0';
    if (auto byte_read = read_n(fd, buf.get(), actual_msg_size);
        byte_read != actual_msg_size) {
      return {byte_read, nullptr};
    }

    return {actual_msg_size, std::move(buf)};
  }
};
