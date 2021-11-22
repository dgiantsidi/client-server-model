#include "client_message.pb.h"

// TODO:
// 1. decode the request (deserialize using protobufs)
// 2. execute the request
inline void func1(size_t sz /*size*/, char * buf /*buffer*/) {
  sockets::client_msg msg;
  size_t payload_size = sz - 4;
  std::string tmp(buf + 4, payload_size);
  msg.ParseFromString(tmp);

  for (auto i = 0; i < msg.ops_size(); i++) {
    const sockets::client_msg::OperationData & op = msg.ops(i);

    switch (op.type()) {
      case sockets::client_msg::PUT: {
        break;
      }
      case sockets::client_msg::GET: {
        break;
      }
      case sockets::client_msg::TXN_START: {
        break;
      }
      case sockets::client_msg::TXN_PUT: {
        break;
      }
      case sockets::client_msg::TXN_GET: {
        break;
      }
      case sockets::client_msg::TXN_COMMIT: {
        break;
      }
      case sockets::client_msg::TXN_ABORT: {
        break;
      }
      default:
        exit(-2);
    }
  }
}
