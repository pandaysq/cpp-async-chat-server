#include <cerrno>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace {
int connect_to(const char *host, const char *port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo *addresses = nullptr;
    const int status = getaddrinfo(host, port, &hints, &addresses);
    if (status != 0) throw std::runtime_error(gai_strerror(status));
    int connection = -1;
    for (addrinfo *entry = addresses; entry != nullptr; entry = entry->ai_next) {
        connection = socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol);
        if (connection < 0) continue;
        if (connect(connection, entry->ai_addr, entry->ai_addrlen) == 0) break;
        close(connection);
        connection = -1;
    }
    freeaddrinfo(addresses);
    if (connection < 0) throw std::runtime_error("Could not connect to chat server");
    return connection;
}

void send_all(int fd, const std::string &text) {
    std::size_t offset = 0;
    while (offset < text.size()) {
        const ssize_t size = send(fd, text.data() + offset, text.size() - offset, MSG_NOSIGNAL);
        if (size < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(std::string("send: ") + std::strerror(errno));
        }
        offset += static_cast<std::size_t>(size);
    }
}
} // namespace

int main(int argc, char **argv) {
    if (argc < 2 || argc > 3) {
        std::cerr << "Usage: chat-client <host> [port]\n";
        return 1;
    }
    try {
        const int fd = connect_to(argv[1], argc == 3 ? argv[2] : "9000");
        std::cout << "Connected. Type nickname and press Enter.\n";
        pollfd watched[2] = {{STDIN_FILENO, POLLIN, 0}, {fd, POLLIN, 0}};
        char buffer[4096];
        while (true) {
            const int ready = poll(watched, 2, -1);
            if (ready < 0) {
                if (errno == EINTR) continue;
                throw std::runtime_error("poll failed");
            }
            if (watched[1].revents & (POLLHUP | POLLERR | POLLNVAL)) break;
            if (watched[1].revents & POLLIN) {
                const ssize_t size = recv(fd, buffer, sizeof(buffer), 0);
                if (size <= 0) break;
                std::cout.write(buffer, size).flush();
            }
            if (watched[0].revents & POLLIN) {
                std::string line;
                if (!std::getline(std::cin, line)) break;
                send_all(fd, line + "\n");
                if (line == "/quit") break;
            }
        }
        close(fd);
        std::cout << "Disconnected.\n";
    } catch (const std::exception &error) {
        std::cerr << "Error: " << error.what() << '\n';
        return 1;
    }
}