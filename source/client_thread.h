class client_thread {
    public:
        void connect_to_the_server(int port, char* hostname) 
        {

            struct hostent *he;
            if ((he = gethostbyname(hostname)) == NULL) {
                exit(1);
            }

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

            their_addr.sin_family   = AF_INET;
            their_addr.sin_port     = htons(port);
            their_addr.sin_addr     = *((struct in_addr *)he->h_addr);
            memset(&(their_addr.sin_zero), 0, 8);

            if (connect(sockfd, (struct sockaddr *)&their_addr, sizeof(struct sockaddr)) == -1) {
                perror("connect");
                exit(1);
            }

        }

        void sent_request(char* request, size_t size) {
            int numbytes;
            if ((numbytes = secure_send(sockfd, request, size)) == -1) {
                std::cout << std::strerror(errno) << "\n";
                exit(1);
            }

        }

        ~client_thread() {
            ::close(sockfd);
        }

    private:
        int sockfd;
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
};
