#pragma once

#include <cstdint>
#include <cstring>

#if !defined(LITTLE_ENDIAN)
#  if defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__)
#    define LITTLE_ENDIAN __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#  else
#    define LITTLE_ENDIAN true
#  endif
#endif

static constexpr auto length_size_field = sizeof(uint32_t);
static constexpr auto client_base_addr = 30500;

/**
 ** It takes as an argument a ptr to an array of size 4 or bigger and
 ** converts the char array into an integer.
 **/
inline auto convert_byte_array_to_int(char * b) noexcept -> uint32_t {
  if constexpr (LITTLE_ENDIAN) {
#if defined(__GNUC__)
    uint32_t res = 0;
    memcpy(&res, b, sizeof(res));
    return __builtin_bswap32(res);
#else  // defined(__GNUC__)
    return (b[0] << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8)
        | (b[3] & 0xFF);
#endif  // defined(__GNUC__)
  }
  uint32_t result = 0;
  memcpy(&result, b, sizeof(result));
  return result;
}

/**
 ** It takes as arguments one char[] array of 4 or bigger size and an integer.
 ** It converts the integer into a byte array.
 **/
inline auto convert_int_to_byte_array(char * dst, uint32_t sz) noexcept
    -> void {
  if constexpr (LITTLE_ENDIAN) {
#if defined(__GNUC__)
    auto tmp = __builtin_bswap32(sz);
    memcpy(dst, &tmp, sizeof(tmp));
#else  // defined(__GNUC__)
    auto tmp = dst;
    tmp[0] = (sz >> 24) & 0xFF;
    tmp[1] = (sz >> 16) & 0xFF;
    tmp[2] = (sz >> 8) & 0xFF;
    tmp[3] = sz & 0xFF;
#endif  // defined(__GNUC__)
  } else {  // BIG_ENDIAN
    memcpy(dst, &sz, sizeof(sz));
  }
}

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
    } else if (bytes_read_now > 0) {
      bytes_read += bytes_read_now;
    }
  }
  return bytes_read;
}

static auto secure_recv(int fd) -> std::pair<size_t, std::unique_ptr<char[]>> {
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
