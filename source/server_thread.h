#pragma once
#include <thread>
#include <mutex>
#include <algorithm>
#include <condition_variable>

#include "callback.h"

class server_thread {
    public:
        server_thread() = delete;
        explicit server_thread(int i): id(i) {};


        server_thread(const server_thread& other) = delete;
        void operator=(const server_thread& other) = delete;

        server_thread(const server_thread&& other) {
            std::cout << __PRETTY_FUNCTION__ << "\n";
        }

        void operator=(const server_thread&& other) {
            std::cout << __PRETTY_FUNCTION__ << "\n";

        }

        void update_connections(int new_sock_fd) {
            {
                std::unique_lock<std::mutex> lock(mtx);
                listening_sockets.push_back(new_sock_fd);
            }
            cv.notify_one();
        };


        int incomming_requests()
        {
            struct timeval tv;
            tv.tv_sec = 10;
            tv.tv_usec = 0;
            int retval = -1;

            retval = select((max_fd+1), &rfds, NULL, NULL, &tv);
            if (retval == 0) {
                std::cout << "update connections\n";
                init();
            }
            else if (retval < 0) {
                std::cout << "timeout\n";
            }
            return retval;
        }

        void cleanup_connection(int dead_connection) {
            // cleanup connection 
            std::unique_lock<std::mutex> lock(mtx);
            auto it = std::find(listening_sockets.begin(), listening_sockets.end(), dead_connection);
            listening_sockets.erase(it);
            std::cout << __PRETTY_FUNCTION__ << ": " << dead_connection << "\n";
        }


        int get_new_requests(void (*process_req) (size_t, char*)) 
        {
            std::vector<int> lsockets;
            {
                std::unique_lock<std::mutex> lock(mtx);
                lsockets = listening_sockets;
            }
            uint64_t bytecount = 0;
            std::unique_ptr<char[]> buffer;
            for (auto csock : lsockets) {
                if (FD_ISSET(csock, &rfds)) {
                    if ((bytecount = secure_recv(csock, buffer))  <= 0) {
                        if (bytecount == 0) {
                            cleanup_connection(csock);
                            init();
                        }
                    }
                    process_req(bytecount, buffer.get());

                }
            }
        }

        void init() {
            get_new_connections();
            reset_fds();
        }


    private:
        int id, max_fd;
        fd_set rfds;
        std::vector<int> listening_sockets;
        std::mutex mtx;
        std::condition_variable cv;

        void get_new_connections() {
            std::unique_lock<std::mutex> lock(mtx);
            auto nb_connections = listening_sockets.size();
            while (nb_connections == 0) {
                std::cout << __PRETTY_FUNCTION__ << ": no connections \n";
                cv.wait(lock);
                nb_connections = listening_sockets.size();
            }
        }

        void reset_fds() {
            max_fd = -1;
            FD_ZERO(&rfds);
            for (auto rfd : listening_sockets) {
                FD_SET(rfd, &rfds);
                max_fd = (max_fd < rfd) ? rfd : max_fd;
            }
        }

        /**
         *  * It takes as an argument a ptr to an array of size 4 or bigger and 
         *   * converts the char array into an integer.
         *    */
        int convertByteArrayToInt(char* b) {
            return (b[0] << 24)
                + ((b[1] & 0xFF) << 16)
                + ((b[2] & 0xFF) << 8)
                + (b[3] & 0xFF);

        }

        /**
         *  * It returns the actual size of msg.
         *   * Not that msg might not contain all payload data. 
         *    * The function expects at least that the msg contains the first 4 bytes that
         *     * indicate the actual size of the payload.
         *      */
        int destruct_message(char* msg, size_t bytes) {
            if (bytes < 4)
                return -1;

            auto actual_msg_size = convertByteArrayToInt(msg);

            return actual_msg_size;

        }


        int secure_recv(int fd, std::unique_ptr<char[]>& buf) {
            int64_t bytes = 0;
            char* tmp;
            int64_t remaining_bytes = 4;
            char dlen[4];
            tmp = dlen;

            while (remaining_bytes > 0) {
                bytes = recv(fd, tmp, remaining_bytes, 0);
                if (bytes < 0) {
                    // @dimitra: Note that the socket is non-blocking so it is fine to return -1 (EWOULDBLOCK/EAGAIN).
                }
                else if (bytes == 0) {
                    // @dimitra: Connection reset by peer
                    return 0;
                }
                else {
                    remaining_bytes -= bytes;
                    tmp += bytes;
                }
            }

            int64_t actual_msg_size = destruct_message(dlen, 4);
            remaining_bytes = actual_msg_size;
            buf = std::make_unique<char[]>(actual_msg_size+1);
            buf[actual_msg_size] = '\0';
            tmp = buf.get();

            while (remaining_bytes > 0) {
                bytes = recv(fd, tmp, remaining_bytes, 0);
                if (bytes < 0) {
                    // @dimitra: Note that the socket is non-blocking so it is fine to return -1 (EWOULDBLOCK/EAGAIN).
                }
                else if (bytes == 0) {
                    return 0;
                }
                else {
                    remaining_bytes -= bytes;
                    tmp += bytes;
                }
            }
            return actual_msg_size;
        }
};
