#include <iostream>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>


#include <memory>
#include <thread>
#include <vector>
#include <fcntl.h>


#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <signal.h>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <memory>
#include <atomic>
#include "client_message.pb.h"


#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>

int nb_clients  = -1;
int port        = -1;
int nb_messages = -1;

uint64_t last_result = 0;

std::string _string = "llllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll";
std::atomic<uint32_t> global_number{0};

/**
 *  * It takes as arguments one char[] array of 4 or bigger size and an integer.
 *   * It converts the integer into a byte array.
 *    */
void _convertIntToByteArray(char* dst, int sz) {
    auto tmp = dst;
    tmp[0] = (sz >> 24) & 0xFF;
    tmp[1] = (sz >> 16) & 0xFF;
    tmp[2] = (sz >> 8) & 0xFF;
    tmp[3] = sz & 0xFF;

}

/**
 *  * It takes as an argument a ptr to an array of size 4 or bigger and 
 *   * converts the char array into an integer.
 *    */
int _convertByteArrayToInt(char* b) {
    return (b[0] << 24)
        + ((b[1] & 0xFF) << 16)
        + ((b[2] & 0xFF) << 8)
        + (b[3] & 0xFF);

}

std::unique_ptr<char[]> get_operation(size_t& size) {
    static int i = 0;
    static int j = 1;
    if (j > 100) {
        j = 0;

    }

    sockets::client_msg _msg;
    if (i % 3 == 0) {
        i++;
        for (int k = 0; k < j; k++) {
            sockets::client_msg::OperationData* op = _msg.add_ops();
            op->set_argument(1);
            global_number.fetch_add(1);
            op->set_type(sockets::client_msg::ADD);

        }
        j++;

        std::string msg_str;
        _msg.SerializeToString(&msg_str);
        char number[4];
        size_t sz = msg_str.size();
        _convertIntToByteArray(number, sz);
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(sz+4);
        ::memcpy(buf.get(), number, 4);

        ::memcpy(buf.get()+4, msg_str.c_str(), sz);
        size = sz + 4;

        return std::move(buf);


    }
    else if (i % 3 == 1){
        i++;
        for (int k = 0; k < j; k++) {
            sockets::client_msg::OperationData* op = _msg.add_ops();
            op->set_argument(1);
            global_number.fetch_sub(1);
            op->set_type(sockets::client_msg::SUB);

        }
        j++;

        std::string msg_str;
        _msg.SerializeToString(&msg_str);

        char number[4];
        size_t sz = msg_str.size();
        _convertIntToByteArray(number, sz);
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(sz+4);
        ::memcpy(buf.get(), number, 4);
        ::memcpy(buf.get()+4, msg_str.c_str(), sz);
        size = sz + 4;
        return std::move(buf);


    }
    else {
        i++;
        for (int k = 0; k < j; k++) {
            sockets::client_msg::OperationData* op = _msg.add_ops();
            op->set_random_data(_string);
            op->set_type(sockets::client_msg::RANDOM_DATA);

        }
        j++;

        std::string msg_str;
        _msg.SerializeToString(&msg_str);

        char number[4];
        size_t sz = msg_str.size();
        _convertIntToByteArray(number, sz);
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(sz+4);
        ::memcpy(buf.get(), number, 4);
        ::memcpy(buf.get()+4, msg_str.c_str(), sz);
        size = sz + 4;
        return std::move(buf);



    }


}



/**
 *  * It constructs the message to be sent. 
 *   * It takes as arguments a destination char ptr, the payload (data to be sent)
 *    * and the payload size.
 *     * It returns the expected message format at dst ptr;
 *      *
 *       *  |<---msg size (4 bytes)--->|<---payload (msg size bytes)--->|
 *        *
 *         *
 *          */
void construct_message(char* dst, char* payload, size_t payload_size) {
    _convertIntToByteArray(dst, payload_size);
    ::memcpy(dst+4, payload, payload_size);
}

/**
 *  * Sends to the connection defined by the fd, a message with a payload (data) of size len bytes.
 *   */
int secure_send(int fd, char* data, size_t len) {
    int64_t bytes = 0;
    int64_t remaining_bytes = len + 4;

    std::unique_ptr<char[]> buf = std::make_unique<char[]>(len + 4);
    construct_message(buf.get(), data, len);
    char* tmp = buf.get();

    while (remaining_bytes > 0) {
        bytes = send(fd, tmp, remaining_bytes, 0);
        if (bytes < 0) {
            // @dimitra: the socket is in non-blocking mode; select() should be also applied 
            //             return -1;
            //                     
        }
        remaining_bytes -= bytes;
        tmp += bytes;

    }

    return len;
}


void client(void* args) {
    int sockfd, numbytes;

    // connector.s address information
    struct sockaddr_in their_addr; 

    //***************************block of code finds the localhost IP
    char hostn[400]; //placeholder for the hostname

    struct hostent *hostIP; //placeholder for the IP address

    //if the gethostname returns a name then the program will get the ip address using gethostbyname
    if((gethostname(hostn, sizeof(hostn))) == 0) {
        hostIP = gethostbyname(hostn); //the netdb.h function gethostbyname
    }
    else {
        printf("ERROR:FC4539 - IP Address not found."); //error if the hostname is not found
    }
    //****************************************************************



    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
        perror("socket");
        exit(1);
    }

    struct hostent *he      = reinterpret_cast<struct hostent*>(args);
    their_addr.sin_family   = AF_INET;
    their_addr.sin_port     = htons(port);
    their_addr.sin_addr     = *((struct in_addr *)he->h_addr);
    memset(&(their_addr.sin_zero), 0, 8);

    if (connect(sockfd, (struct sockaddr *)&their_addr, sizeof(struct sockaddr)) == -1) {
        perror("connect");
        exit(1);
    }

    uint64_t bytes_sent = 0;
    int iterations = nb_messages;
    while (iterations > 0) {
        size_t size = 0;
        std::unique_ptr<char[]> buf = get_operation(size);
        if ((numbytes = secure_send(sockfd, buf.get(), size)) == -1) {
            std::cout << std::strerror(errno) << "\n";
            exit(1);
        }
        bytes_sent += numbytes;
        iterations--;
    }

    close(sockfd);
}


int main (int args, char* argv[]) {

    if (args < 5) {
        std::cerr << "usage: ./client <nb_threads> <hostname> <port> <nb_messages>\n";
        exit(-1);
    }

    nb_clients  = std::atoi(argv[1]);
    port        = std::atoi(argv[3]);
    nb_messages = std::atoi(argv[4]);

    struct hostent *he;
    if ((he = gethostbyname(argv[2])) == NULL) {
        exit(1);

    }

    // creating the client threads
    std::vector<std::thread> threads;

    for (size_t i = 0; i < nb_clients; i++) {
        threads.emplace_back(std::thread(client, he));
    }

    for (auto& thread : threads) {
        thread.join();
    }

    std::cout << "** all threads joined **\n";
}
