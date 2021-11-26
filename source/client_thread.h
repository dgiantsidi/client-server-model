#pragma once

#include <optional>

#include <fmt/format.h>

#include "message.h"
#include "shared.h"

class ClientThread {
public:
  void connect_to_the_server(int port, char const * /*hostname*/) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    hostent * he = hostip;

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
      fmt::print("connect\n");
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }

    // init the listening socket
    int ret = 1;
    port = client_base_addr + thread_id;
    sent_init_connection_request(port);

    int repfd = socket(AF_INET, SOCK_STREAM, 0);
    if (repfd == -1) {
      fmt::print("socket\n");
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }

    if (setsockopt(repfd, SOL_SOCKET, SO_REUSEADDR, &ret, sizeof(int)) == -1) {
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

    if (bind(repfd, reinterpret_cast<sockaddr *>(&my_addr), sizeof(sockaddr))
        == -1) {
      fmt::print("bind\n");
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }
    constexpr int max_backlog = 1024;
    if (listen(repfd, max_backlog) == -1) {
      fmt::print("listen\n");
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }

    socklen_t sin_size = sizeof(sockaddr_in);
    fmt::print("waiting for new connections ..\n");
    sockaddr_in tmp_addr {};
    auto new_fd = accept4(repfd,
                          reinterpret_cast<sockaddr *>(&tmp_addr),
                          &sin_size,
                          SOCK_CLOEXEC);
    if (new_fd == -1) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      fmt::print("accecpt() failed ..{}\n", std::strerror(errno));
    }

    fcntl(new_fd, F_SETFL, O_NONBLOCK);
    fmt::print("accept succeeded on socket {} {} ..\n", new_fd, repfd);
    rep_fd = new_fd;
  }

  void sent_init_connection_request(int port) const {
    sockets::client_msg msg;
    auto * operation_data = msg.add_ops();
    operation_data->set_type(sockets::client_msg::INIT);
    operation_data->set_port(port);
    std::string msg_str;
    msg.SerializeToString(&msg_str);

    auto msg_size = msg_str.size();
    auto buf = std::make_unique<char[]>(msg_size + length_size_field);
    convert_int_to_byte_array(buf.get(), msg_size);
    memcpy(buf.get() + length_size_field,
           msg_str.c_str(),
           msg_size);

    secure_send(sockfd, buf.get(), msg_size + length_size_field);
  }

  void sent_request(char * request, size_t size) const {
    if (auto numbytes = secure_send(sockfd, request, size); !numbytes) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      fmt::print("{}\n", std::strerror(errno));
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }
  }

  void recv_ack() {
    auto [bytecount, buffer] = secure_recv(rep_fd);
    if (buffer == nullptr) {
      printf("ERROR\n");
      exit(2);
    }
    replies++;
  }

  ClientThread() = delete;
  explicit ClientThread(int tid)
      : thread_id(tid) {}
  ClientThread(int port, char const * hostname) {
    connect_to_the_server(port, hostname);
  }

  ClientThread(ClientThread const &) = delete;
  auto operator=(ClientThread const &) -> ClientThread & = delete;

  ClientThread(ClientThread && other) noexcept
      : sockfd(other.sockfd) {
    other.sockfd = -1;
  }

  auto operator=(ClientThread && other) noexcept -> ClientThread & {
    sockfd = other.sockfd;
    other.sockfd = -1;
    return *this;
  }

  ~ClientThread() {
    if (sockfd != -1) {
      ::close(sockfd);
    }
  }

  int32_t replies = 0;

private:
  int sockfd = -4, rep_fd = -1;
  int thread_id = 0;

  /**
  * It constructs the message to be sent.
  * It takes as arguments a destination char ptr, the payload (data to be
   sent)
  * and the payload size.
  * It returns the expected message format at dst ptr;
  *
  *  |<---msg size (4 bytes)--->|<---payload (msg size bytes)--->|
  *
  *
  */
  static void construct_message(char * dst,
                                char * payload,
                                size_t payload_size) {
    convert_int_to_byte_array(dst, payload_size);
    ::memcpy(dst + 4, payload, payload_size);
  }

  /**
     * Sends to the connection defined by the fd, a message with a payload
    (data) of size len bytes.
      */
#if 1
  static auto secure_send(int fd, char * data, size_t len)
      -> std::optional<size_t> {
    auto bytes = 0LL;
    auto remaining_bytes = len + 4;

    std::unique_ptr<char[]> buf = std::make_unique<char[]>(len + 4);
    construct_message(buf.get(), data, len);
    char * tmp = buf.get();

    while (remaining_bytes > 0) {
      bytes = send(fd, tmp, remaining_bytes, 0);
      if (bytes < 0) {
        // @dimitra: the socket is in non-blocking mode; select() should be also
        // applied
        //             return -1;
        //
        return std::nullopt;
      }
      remaining_bytes -= bytes;
      tmp += bytes;
    }

    return len;
  }
#endif
};
