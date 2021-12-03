#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>

#include "generate_traces.h"

namespace Workload {
namespace {

auto split(std::string const & s, char delimiter) -> std::vector<std::string> {
  std::vector<std::string> tokens;
  std::string token;
  std::istringstream token_stream(s);
  while (std::getline(token_stream, token, delimiter)) {
    tokens.push_back(token);
  }
  return tokens;
}

auto parse_trace(uint16_t unused /* unused */,
                 const std::string & path,
                 int read_permille) -> std::vector<TraceCmd> {
  std::ifstream file(path);
  if (!file) {
    return {};
  }

  std::vector<TraceCmd> res;
  res.reserve(1000000);
  std::string line;
  while (std::getline(file, line)) {
    if (line.length() == 0) {
      continue;
    }

    if (line[line.length() - 1] == '\n') {
      line[line.length() - 1] = 0;
    }

    auto elements = split(line, ' ');
    if (elements.empty()) {
      continue;
    }
    res.emplace_back(elements[0], read_permille);
  }
  return res;
}

auto manufacture_trace(uint16_t unused /* unused */,
                       size_t trace_size,
                       size_t nb_keys,
                       int read_permille) -> std::vector<TraceCmd> {
  std::vector<TraceCmd> res;
  res.reserve(trace_size);
  for (auto i = 0ULL; i < trace_size; ++i) {
    res.emplace_back(static_cast<uint32_t>(rand() % nb_keys), read_permille);
  }
  return res;
}

}  // namespace

void TraceCmd::init(uint32_t key_id, int read_permille) {
  op = (rand() % 1000) < read_permille ? get : put;
  // memcpy(key_hash, &key_id, sizeof(key_id));
  key_hash = key_id;
}

TraceCmd::TraceCmd(uint32_t key_id, int read_permille) {
  init(key_id, read_permille);
}

TraceCmd::TraceCmd(std::string const & s, int read_permille) {
  init(static_cast<uint32_t>(strtoul(s.c_str(), nullptr, 10)), read_permille);
}

auto trace_init(uint16_t t_id, const std::string & path)
    -> std::vector<TraceCmd> {
  return parse_trace(t_id, path, default_read_permille);
}

auto trace_init(const std::string & file_path, int read_permile)
    -> std::vector<TraceCmd> {
  return parse_trace(0, file_path, read_permile);
}

auto trace_init(uint16_t t_id,
                size_t trace_size,
                size_t nb_keys,
                int read_permille,
                int rand_start) -> std::vector<TraceCmd> {
  srand(rand_start);
  return manufacture_trace(t_id, trace_size, nb_keys, read_permille);
}

}  // namespace Workload
