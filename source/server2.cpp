#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>

#include <arpa/inet.h>
#include <cxxopts.hpp>
#include <fcntl.h>
#include <fmt/format.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "kv_store.h"
#include "server_thread2.h"
#include <vector>

constexpr std::string_view usage = "usage: ./server <nb_server_threads> <port>";

// how many pending connections the queue will hold?
constexpr int backlog = 1024;

auto construct_reply(int op_id, int success, int txn_id, std::string_view val)
	-> std::unique_ptr<char[]> {
		server::server_response::reply rep;
		rep.set_op_id(op_id);
		rep.set_success(success);
		rep.set_txn_id(txn_id);
		rep.set_value(val.data(), val.size());
		// fmt::print("{} value={}\n", __func__, rep.value());

		std::string msg_str;
		rep.SerializeToString(&msg_str);

		auto msg_size = msg_str.size();
		auto buf = std::make_unique<char[]>(msg_size + length_size_field);
		construct_message(buf.get(), msg_str.c_str(), msg_size);
		return buf; // copy-elision
	}

void process_put(KvStore &db, ServerThread *args,
		sockets::client_msg::OperationData const &op, int fd) {
	// fmt::print("{} key={}, value={}\n", __func__, op.key(), op.value());
	auto success = db.put(op.key(), op.value());

	auto rep_ptr = construct_reply(op.op_id(), success, -1 /* not a txn */,
			"" /* empty val */);
	args->enqueue_reply(fd, std::move(rep_ptr));
}

void process_get(KvStore const &db, ServerThread *args,
		sockets::client_msg::OperationData const &op, int fd) {
	std::string val;
	auto ret_val = db.get(op.key(), val);

	//	fmt::print("{} key={}, val={}\n", __func__, op.key(), val);
	auto rep_ptr = [&ret_val, &op]() {
		if (!ret_val) {
			//     fmt::print("Key: {} not found\n", op.key());
			return construct_reply(op.op_id(), 0, -1, "");
		}
		return construct_reply(op.op_id(), 1, -1, *ret_val);
	}();

	args->enqueue_reply(fd, std::move(rep_ptr));
}

void process_tx(KvStore &db, ServerThread *args,
		sockets::client_msg::OperationData const &op, int fd) {
	fmt::print("[{}] no tx-workload\n", __func__);
	exit(128);
}

static void processing_func(KvStore &db, ServerThread *args) {
	auto constexpr func_name = __func__;
	fmt::print("[{}] args={}\n", __func__, (void*)args);
	args->init();
	args->register_callback(
			sockets::client_msg::PUT,
			[&db, args](auto const &op, int fd) { process_put(db, args, op, fd); });
	args->register_callback(
			sockets::client_msg::GET,
			[&db, args](auto const &op, int fd) { process_get(db, args, op, fd); });
	args->register_callback(
			sockets::client_msg::TXN_START,
			[&db, args](auto const &op, int fd) { process_tx(db, args, op, fd); });
	args->register_callback(
			sockets::client_msg::TXN_GET,
			[&db, args](auto const &op, int fd) { process_tx(db, args, op, fd); });
	args->register_callback(
			sockets::client_msg::TXN_PUT,
			[&db, args](auto const &op, int fd) { process_tx(db, args, op, fd); });
	args->register_callback(
			sockets::client_msg::TXN_COMMIT,
			[&db, args](auto const &op, int fd) { process_tx(db, args, op, fd); });
	args->register_callback(
			sockets::client_msg::TXN_ABORT,
			[&db, args](auto const &op, int fd) { process_tx(db, args, op, fd); });

	for (bool should_continue = true; should_continue;) {
		auto ret = args->incomming_requests();
		if (ret > 0) {
			args->get_new_requests();
			args->post_replies();
		}


	}
}

auto main(int argc, char *argv[]) -> int {
	KvStore db("0");
	cxxopts::Options options("svr", "Example server for the sockets benchmark");
	options.allow_unrecognised_options().add_options()(
			"n,s_threads", "Number of threads", cxxopts::value<size_t>())(
			"s,hostname", "Hostname of the server", cxxopts::value<std::string>())(
			"p,port", "Port of the server", cxxopts::value<size_t>())(
			"o,one_run", "Run only one time", cxxopts::value<bool>())(
			"c,c_threads", "Number of clients",
			cxxopts::value<size_t>()->default_value("0"))("h,help", "Print help");

	auto args = options.parse(argc, argv);

	if (args.count("help")) {
		fmt::print("{}\n", options.help());
		return 0;
	}

	if (!args.count("s_threads")) {
		fmt::print(stderr, "The number of threads n_threads is required\n{}\n",
				options.help());
		return -1;
	}

	if (!args.count("hostname")) {
		fmt::print(stderr, "The hostname is required\n{}\n", options.help());
		return -1;
	}

	if (!args.count("port")) {
		fmt::print(stderr, "The port is required\n{}\n", options.help());
		return -1;
	}

	if (!args.count("c_threads")) {
		fmt::print(stderr, "The number of clients is required\n{}\n",
				options.help());
		return -1;
	}

	auto const nb_server_threads = args["s_threads"].as<size_t>();
	if (nb_server_threads == 0) {
		fmt::print(stderr, "{}\n", usage);
		return 1;
	}
	auto port = args["port"].as<size_t>();

	auto one_run = args["one_run"].as<bool>();
	auto total_clients = args["c_threads"].as<size_t>();

	std::vector<ServerThread*> server_threads;
	std::vector<std::thread> threads;
	// threads.reserve(nb_server_threads);

	for (size_t i = 0; i < nb_server_threads; i++) {
		server_threads.emplace_back(new ServerThread(i));
		fmt::print("[{}] args={}\n", __func__, (void*)server_threads[i]);
		// threads.emplace_back(processing_func, std::ref(db), &server_threads[i]);
	}
	for (size_t i = 0; i < nb_server_threads; i++) {
		// server_threads.emplace_back(i);
		// fmt::print("[{}] args={}\n", __func__, (void*)&server_threads[i]);
		threads.emplace_back(processing_func, std::ref(db), server_threads[i]);
	}

	int ret = 1;

	int sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd == -1) {
		fmt::print("socket\n");
		// NOLINTNEXTLINE(concurrency-mt-unsafe)
		exit(1);
	}

	if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &ret, sizeof(int)) == -1) {
		fmt::print("setsockopt\n");
		// NOLINTNEXTLINE(concurrency-mt-unsafe)
		exit(1);
	}

	sockaddr_in my_addr{};
	my_addr.sin_family = AF_INET;         // host byte order
	my_addr.sin_port = htons(port);       // short, network byte order
	my_addr.sin_addr.s_addr = INADDR_ANY; // automatically fill with my IP
	memset(&(my_addr.sin_zero), 0,
			sizeof(my_addr.sin_zero)); // zero the rest of the struct

	if (bind(sockfd, reinterpret_cast<sockaddr *>(&my_addr), sizeof(sockaddr)) ==
			-1) {
		fmt::print("bind\n");
		// NOLINTNEXTLINE(concurrency-mt-unsafe)
		exit(1);
	}

	if (listen(sockfd, backlog) == -1) {
		fmt::print("listen\n");
		// NOLINTNEXTLINE(concurrency-mt-unsafe)
		exit(1);
	}

	uint64_t nb_clients = 0;
	for (;;) {
		socklen_t sin_size = sizeof(sockaddr_in);
		fmt::print("waiting for new connections ..\n");
		sockaddr_in their_addr{};
		if (one_run) {
			if (nb_clients == total_clients) {
				break;
			}
		}
		auto new_fd = accept4(sockfd, reinterpret_cast<sockaddr *>(&their_addr),
				&sin_size, SOCK_CLOEXEC);
		if (new_fd == -1) {
			// NOLINTNEXTLINE(concurrency-mt-unsafe)
			fmt::print("accecpt() failed ..{}\n", std::strerror(errno));
			continue;
		}

		fmt::print("Received request from Client: {}:{}\n",
				inet_ntoa(their_addr.sin_addr), // NOLINT(concurrency-mt-unsafe)
				port);
		{
			auto server_thread_id = nb_clients % nb_server_threads;
			fmt::print("socket : {}  matched to thread: {}\n", new_fd,
					server_thread_id);
			fcntl(new_fd, F_SETFL, O_NONBLOCK);
			server_threads[server_thread_id]->update_connections(new_fd);
			nb_clients++;
		}
	}

	fmt::print("[{}] nb_clients={}/total_clients={}\n", __func__, nb_clients, total_clients);
	std::atomic_thread_fence(std::memory_order_release);
	for (auto &t : server_threads) {
		t->should_exit.store(true, std::memory_order_relaxed);
	}

	for (auto &th : threads) {
		th.join();
	}

	fmt::print("[{}] all threads joined .. success\n", __func__);

	return 0;
}
