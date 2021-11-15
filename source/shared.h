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

/**
 *  * It takes as an argument a ptr to an array of size 4 or bigger and
 *  * converts the char array into an integer.
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
 *  * It takes as arguments one char[] array of 4 or bigger size and an integer.
 *   * It converts the integer into a byte array.
 *    */
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