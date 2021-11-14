#include <thread>
#include <vector>
#include <iostream>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>


#include "server_thread.h"

int nb_server_threads   = 1;
int port                = 1025;
constexpr int backlog   = 1024; // how many pending connections the queue will hold


static void processing_func(std::shared_ptr<class server_thread> args)
{
    args->init();
    while (1)
    {
        int ret = args->incomming_requests();

        if (ret > 0) {
            // new req 
            args->get_new_requests(func1);
            continue;
        }

    }
}

int main(int args, char* argv[]) {
    if (args < 5) {
        std::cerr << "usage: ./server <nb_server_threads> <port>\n";
    }

    nb_server_threads = std::atoi(argv[1]);
    port = std::atoi(argv[2]);

std::vector<std::shared_ptr<class server_thread>> server_threads;
server_threads.reserve(nb_server_threads);
std::vector<std::thread> threads;
threads.reserve(nb_server_threads);

    for (size_t i = 0; i < nb_server_threads; i++) {
        auto ptr = std::make_shared<class server_thread>(i);
        server_threads.push_back(ptr);
        threads.emplace_back(std::thread(processing_func, server_threads[i]));
    }

    /* listen on sock_fd, new connection on new_fd */
    int sockfd, new_fd; 

    /* my address information */
    struct sockaddr_in my_addr; 

    /* connector.s address information */
    struct sockaddr_in their_addr; 
    socklen_t sin_size;

    int ret = 1;

    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("socket");
        exit(1);

    }

    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &ret, sizeof(int)) == -1){
        perror("setsockopt");
        exit(1);

    }

    my_addr.sin_family      = AF_INET;          // host byte order
    my_addr.sin_port        = htons(port);    // short, network byte order
    my_addr.sin_addr.s_addr = INADDR_ANY;       // automatically fill with my IP
    memset(&(my_addr.sin_zero), 0, 8);          // zero the rest of the struct

    if (bind(sockfd, (struct sockaddr *)&my_addr, sizeof(struct sockaddr)) == -1) {
        perror("bind");
        exit(1);

    }

    if (listen(sockfd, backlog) == -1) {
        perror("listen");
        exit(1);

    }


    uint64_t nb_clients = 0;
    while(1) {
        sin_size = sizeof(struct sockaddr_in);
        std::cout << "waiting for new connections ..\n";
        if ((new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size)) == -1) {
            std::cerr << "accept() failed .. " << std::strerror(errno) << "\n";
            continue;

        }

        printf("Received request from Client: %s:%d\n", inet_ntoa(their_addr.sin_addr), port);
        {
            auto server_thread_id = nb_clients % nb_server_threads;
            std::cout << "socket : " << new_fd << " matched to thread: " << server_thread_id << "\n";
            fcntl(new_fd, F_SETFL, O_NONBLOCK);
            server_threads[server_thread_id]->update_connections(new_fd);
            /*
               std::unique_lock<std::mutex> lk(threads_args[nb_clients % nb_server_threads].get()->mtx);
               threads_args[nb_clients % nb_server_threads].get()->listening_socket.push_back(new_fd);
               threads_args[nb_clients % nb_server_threads].get()->total_bytes.insert({new_fd, 0});
               threads_args[nb_clients % nb_server_threads].get()->cv.notify_one();
               */
            nb_clients++;

        }

    }

    return 0;

}
