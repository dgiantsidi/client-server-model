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
#include <netinet/in.h>
#include <sys/socket.h>
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

  auto operator=(ServerThread && other) noexcept -> ServerThread &;

  inline ~ServerThread() {
    std::lock_guard lock(mtx);
    // TODO: close all sockets
    if (id != -1) {
      close(id);
    }
  }

  void update_connections(int new_sock_fd);

  auto incomming_requests() -> int;

  void cleanup_connection(int dead_connection);

  inline void register_callback(sockets::client_msg::OperationType op,
                                CallbackT cb) {
    callbacks[op] = std::move(cb);
  }

  auto get_new_requests() -> int;

  void init();

private:
  int id;
  int max_fd;
  fd_set rfds;
  CallbackArrayT callbacks = Details::create_array(
      CallbackT {Details::unimplemented}, std::make_index_sequence<n_ops>());
  std::vector<int> listening_sockets;
  std::mutex mtx;
  std::condition_variable cv;

  void get_new_connections();

  void reset_fds();

  /**
   ** It returns the actual size of msg.
   ** Not that msg might not contain all payload data.
   ** The function expects at least that the msg contains the first 4 bytes that
   ** indicate the actual size of the payload.
   **/
  static auto destruct_message(char * msg, size_t bytes)
      -> std::optional<uint32_t>;

  static auto read_n(int fd, char * buffer, size_t n) -> size_t;

  static auto secure_recv(int fd)
      -> std::pair<uint32_t, std::unique_ptr<char[]>>;

  inline auto process_req(size_t sz, char * buf) const -> void;
};
