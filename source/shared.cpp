#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

#include "shared.h"

#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

inline auto destruct_message(char * msg, size_t bytes)
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
    // negative return_val means that there are no more data (fine for non
    // blocking socket)
    if (bytes_read_now == 0) {
      return bytes_read_now;
    }
    if (bytes_read_now > 0) {
      bytes_read += bytes_read_now;
    }
  }
  return bytes_read;
}

auto secure_recv(int fd) -> std::pair<size_t, std::unique_ptr<char[]>> {
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

auto secure_send(int fd, char * data, size_t len) -> std::optional<size_t> {
  auto bytes = 0LL;
  auto remaining_bytes = len;

  char * tmp = data;

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