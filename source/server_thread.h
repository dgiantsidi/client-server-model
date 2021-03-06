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

inline void unimplemented(sockets::client_msg::OperationData const & msg,
                          int fd) {
  auto const & op = msg.type();
  auto const & error_str =
      fmt::format("Unimplemented operation: {} {}",
                  sockets::client_msg::OperationType_Name(op),
                  fd);
  throw std::runtime_error(error_str);
}
}  // namespace Details

class ServerThread {
public:
  struct Timeout {};

  constexpr static size_t n_ops = sockets::client_msg::OperationType_ARRAYSIZE;
  using CallbackT =
      std::function<void(sockets::client_msg::OperationData const &, int fd)>;
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

  auto incomming_requests() -> std::variant<int, Timeout, ErrNo>;

  void cleanup_connection(int dead_connection);
  void create_communication_pair(int listening_socket);

  inline void register_callback(sockets::client_msg::OperationType op,
                                CallbackT cb) {
    callbacks[op] = std::move(cb);
  }

  auto get_new_requests() -> int;
  void enqueue_reply(int fd, std::unique_ptr<char[]> rep);
  void post_replies();

  void init();

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  std::atomic<bool> should_exit {false};

private:
  int id;
  int max_fd;
  fd_set rfds;
  CallbackArrayT callbacks = Details::create_array(
      CallbackT {Details::unimplemented}, std::make_index_sequence<n_ops>());
  std::vector<int> listening_sockets;
  std::unordered_map<int, int> communication_pairs;
  // debbuging
  std::unordered_map<int, int> reqs_per_socket;
  std::mutex mtx;
  std::condition_variable cv;

  std::vector<std::tuple<int, std::unique_ptr<char[]>>> queue_with_replies;

  void get_new_connections();

  void reset_fds();

  static void sent_request(int sockfd, char * request, size_t size) {
    if (auto numbytes = secure_send(sockfd, request, size); !numbytes) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      fmt::print("{}\n", std::strerror(errno));
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }
  }

  // TODO: We might want process_req to actually own the memory...
  //       ... but we need to make sure that the message format than own the
  //       memory so I assume we need to make a class which inherits from
  //       OperationType...
  inline auto process_req(int fd, size_t sz, char * buf) const -> void;
};
