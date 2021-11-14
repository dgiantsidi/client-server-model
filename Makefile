
.PHONY: server client clean

target=all
all: clean server client

server:
	g++ -g -s -fsanitize=address server.cc -o server -lpthread -fsanitize=address

client:
	g++ -Wall -g -L/usr/local/lib client_message.pb.cc client.cc -o client -lprotobuf -lpthread

clean:
	-rm -f server client
