#include <cstdint>
#include <cstring>
#include <optional>
#include <variant>
#include <vector>

#include "server_thread2.h"

#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#if 0
auto ServerThread::operator=(ServerThread &&other) noexcept -> ServerThread & {
	fmt::print("{}\n", __PRETTY_FUNCTION__);
	exit(-1);
	return *this;
}
#endif

void ServerThread::create_communication_pair(int listening_socket) {
	auto *he = hostip;

	auto [bytecount, buffer] = secure_recv(listening_socket);
	if (bytecount == 0) {
		fmt::print("Error on {}\n", __func__);
		// NOLINTNEXTLINE(concurrency-mt-unsafe)
		exit(1);
	}

	sockets::client_msg msg;
	auto payload_sz = bytecount;
	std::string tmp(buffer.get(), payload_sz);
	msg.ParseFromString(tmp);

	fmt::print("done here .. {}\n", msg.ops(0).port());

	int sockfd = -1;
	int port = msg.ops(0).port();
	if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
		fmt::print("socket {}\n", std::strerror(errno));
		// NOLINTNEXTLINE(concurrency-mt-unsafe)
		exit(1);
	}

	// connector.s address information
	sockaddr_in their_addr{};
	their_addr.sin_family = AF_INET;
	their_addr.sin_port = htons(port);
	their_addr.sin_addr = *(reinterpret_cast<in_addr *>(he->h_addr));
	memset(&(their_addr.sin_zero), 0, sizeof(their_addr.sin_zero));

	bool successful_connection = false;
	for (size_t retry = 0; retry < number_of_connect_attempts; retry++) {
		if (connect(sockfd, reinterpret_cast<sockaddr *>(&their_addr),
					sizeof(struct sockaddr)) == -1) {
			// NOLINTNEXTLINE(concurrency-mt-unsafe)
			sleep(1);
		} else {
			successful_connection = true;
			break;
		}
	}
	if (!successful_connection) {
		fmt::print("[{}] could not connect to client after {} attempts ..\n",
				__func__, number_of_connect_attempts);
		exit(1);
	}
	fmt::print("{}<-->{}\n", listening_socket, sockfd);
	communication_pairs.insert({listening_socket, sockfd});
	fmt::print("{}<-->{} finished\n", listening_socket, sockfd);
}

void ServerThread::update_connections(int new_sock_fd) {
	{
		fmt::print("[{}]\n", __func__);
		std::lock_guard lock(mtx);
		fmt::print("[{}]\n", __func__);
		create_communication_pair(new_sock_fd);
		init_conn = 1;
		ev.events = EPOLLIN | EPOLLET;
		ev.data.fd = new_sock_fd;
		epoll_ctl(epfd, EPOLL_CTL_ADD, new_sock_fd, &ev);
	}
	cv.notify_one();
}

auto ServerThread::incomming_requests() -> int {
	// fmt::print("[{}] \n", __func__);
	if (nfds > 0) {
		return nfds;
	}

	while (nfds <= 0) {
		nfds = epoll_wait(epfd, events, 
				MAX_EPOLL_EVENTS_PER_RUN, 
				EPOLL_RUN_TIMEOUT);
		if (nfds < 0) {
			// fmt::print("[{}] timeout\n", __func__);
		}
	}

	return nfds;
	//	return events[--nfds].data.fd;

}

void ServerThread::cleanup_connection(int dead_connection) {
}

auto ServerThread::get_new_requests() -> int {
	{
		// fmt::print("[{}]\n", __func__);
		std::lock_guard lock(mtx);
		for (size_t i = 0ULL; i < nfds; i++) {
			auto csock = events[i].data.fd;
			// fmt::print("[{}] socket={}\n", __func__, csock);
			auto [bytecount, buffer] = secure_recv(csock);
			if (static_cast<int>(bytecount) <= 0) {
				continue;
			} else {
			//	fmt::print("[{}] proc bytecount={}\n", __func__, bytecount);
				process_req(csock, bytecount, buffer.get());
			}
		}
		nfds = 0;
	}
	return 0;
}

auto ServerThread::init() -> void {
	fmt::print("[{}]\n", __func__);
	get_new_connections();
	// reset_fds();
}

void ServerThread::get_new_connections() {
	{
		fmt::print("[{}]\n", __func__);
		std::unique_lock<std::mutex> lock(mtx);
		auto init = init_conn;
		fmt::print("[{}] locked init={}\n", __func__, init);
		while (init== 0) {
			fmt::print("{}: no connections\n", __PRETTY_FUNCTION__);

			using namespace std::chrono_literals;
			cv.wait_for(lock, 1000ms);
			init = init_conn;
			fmt::print("[{}] init={}\n", __func__, init);
			if (init == 0 && should_exit.load(std::memory_order_relaxed)) {
				std::atomic_thread_fence(std::memory_order_acquire);
				debug_print("[{}]: No connection could be established...\n",
						__PRETTY_FUNCTION__);
				return;
			}
		}
	}
}

void ServerThread::reset_fds() {
}

void ServerThread::post_replies() {
	// fmt::print("[{}]\n", __func__);
	for (auto &buf : queue_with_replies) {
		auto it =
			std::find(queue_with_replies.begin(), queue_with_replies.end(), buf);
		auto ptr = std::move(std::get<1>(*it));
		auto msg_size = convert_byte_array_to_int(ptr.get()); // TODO
		auto repfd = communication_pairs[std::get<0>(*it)];
		secure_send(repfd, ptr.get(), msg_size + length_size_field);
	}
	queue_with_replies.clear();
}

auto ServerThread::process_req(int fd, size_t sz, char *buf) const -> void {
	sockets::client_msg msg;
	auto payload_sz = sz;
	std::string tmp(buf, payload_sz);
	msg.ParseFromString(tmp);
	for (auto i = 0; i < msg.ops_size(); ++i) {
		auto const &op = msg.ops(i);
		callbacks[op.type()](op, fd);
	}
}

void ServerThread::enqueue_reply(int fd, std::unique_ptr<char[]> rep) {
	queue_with_replies.emplace_back(fd, std::move(rep));
}
