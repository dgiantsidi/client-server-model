#pragma once

#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>

#include <fmt/format.h>
#include <unistd.h>

#include "callback.h"
#include "shared.h"

class ServerThread {
public:
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
    std::cout << __PRETTY_FUNCTION__ << "\n";
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
    std::cout << __PRETTY_FUNCTION__ << ": " << dead_connection << "\n";
  }

  inline auto get_new_requests(void (*process_req)(size_t, char *)) -> int {
    std::vector<int> lsockets;
    {
      std::lock_guard lock(mtx);
      lsockets = listening_sockets;  // this is weird, you create a copy but do
                                     // not delete old vector?
    }
    // TODO how big do we expect to get this, why not use stack?
    std::unique_ptr<char[]> buffer;
    for (auto csock : lsockets) {
      int64_t bytecount = 0;
      if (FD_ISSET(csock, &rfds)) {  // NOLINT
        if (bytecount = secure_recv(csock, buffer); bytecount <= 0) {
          if (bytecount == 0) {
            cleanup_connection(csock);
            init();
          }
        }
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
  std::vector<int> listening_sockets;
  std::mutex mtx;
  std::condition_variable cv;

  void get_new_connections() {
    std::unique_lock<std::mutex> lock(mtx);
    auto nb_connections = listening_sockets.size();
    while (nb_connections == 0) {
      std::cout << __PRETTY_FUNCTION__ << ": no connections \n";
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

  // FIXME change return type to tuple or even better some array wrapper...
  // NOLINTNEXTLINE(google-runtime-references)
  static auto secure_recv(int fd, std::unique_ptr<char[]> & buf) -> uint32_t {
    int64_t bytes = 0;
    int64_t remaining_bytes = 4;
    char dlen[4];
    auto * tmp = dlen;

    while (remaining_bytes > 0) {
      bytes = recv(fd, tmp, remaining_bytes, 0);
      if (bytes < 0) {
        // @dimitra: Note that the socket is non-blocking so it is fine to
        // return -1 (EWOULDBLOCK/EAGAIN).
        return -1;
      }
      if (bytes == 0) {
        // @dimitra: Connection reset by peer
        return 0;
      }
      remaining_bytes -= bytes;
      tmp += bytes;
    }

    auto actual_msg_size_opt = destruct_message(dlen, 4);
    if (!actual_msg_size_opt) {
      return -1;
    }
    auto actual_msg_size = *actual_msg_size_opt;
    remaining_bytes = actual_msg_size;
    buf = std::make_unique<char[]>(static_cast<size_t>(actual_msg_size) + 1);
    buf[actual_msg_size] = '\0';
    tmp = buf.get();

    while (remaining_bytes > 0) {
      bytes = recv(fd, tmp, remaining_bytes, 0);
      if (bytes < 0) {
        // @dimitra: Note that the socket is non-blocking so it is fine to
        // return -1 (EWOULDBLOCK/EAGAIN).
        return -1;
      }
      if (bytes == 0) {
        return 0;
      }
      remaining_bytes -= bytes;
      tmp += bytes;
    }
    return actual_msg_size;
  }
};
