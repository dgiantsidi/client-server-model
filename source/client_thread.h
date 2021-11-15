#pragma once

#include <optional>

#include <fmt/format.h>

#include "shared.h"

class ClientThread {
public:
  void connect_to_the_server(int port, char const * hostname) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    hostent * he = gethostbyname(hostname);
    if (he == nullptr) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }

    //***************************block of code finds the localhost IP
    constexpr auto max_hostname_length = 512ULL;
    char hostn[max_hostname_length];  // placeholder for the hostname

    //    hostent * hostIP;  // placeholder for the IP address

    // if the gethostname returns a name then the program will get the ip
    // address using gethostbyname
    if ((gethostname(hostn, sizeof(hostn))) == 0) {
      //     hostIP = gethostbyname(hostn);  // the netdb.h function
      //     gethostbyname
    } else {
      fmt::print(
          "ERROR:FC4539 - IP Address not found.\n");  // error if the hostname
                                                      // is not found
    }
    //****************************************************************

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
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      fmt::print("connect");
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }
  }

  void sent_request(char * request, size_t size) const {
    if (auto numbytes = secure_send(sockfd, request, size); !numbytes) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      fmt::print("{}\n", std::strerror(errno));
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      exit(1);
    }
  }

  ClientThread() = default;
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

private:
  int sockfd = -1;
  /**
   *  * It constructs the message to be sent.
   *   * It takes as arguments a destination char ptr, the payload (data to be
   * sent)
   *    * and the payload size.
   *     * It returns the expected message format at dst ptr;
   *      *
   *       *  |<---msg size (4 bytes)--->|<---payload (msg size bytes)--->|
   *        *
   *         *
   *          */
  static void construct_message(char * dst,
                                char * payload,
                                size_t payload_size) {
    convert_int_to_byte_array(dst, payload_size);
    ::memcpy(dst + 4, payload, payload_size);
  }

  /**
   *  * Sends to the connection defined by the fd, a message with a payload
   * (data) of size len bytes.
   *   */
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
};
