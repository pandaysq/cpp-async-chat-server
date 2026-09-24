#include <arpa/inet.h>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace {
volatile std::sig_atomic_t running = 1;
constexpr std::size_t max_line = 4096;
constexpr std::size_t max_pending_output = 1024 * 1024;
constexpr int max_events = 256;

void stop(int) { running = 0; }

void nonblocking(int fd) {
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        throw std::runtime_error(std::string("fcntl: ") + std::strerror(errno));
    }
}

std::string lower(std::string text) {
    for (char &character : text) {
        if (character >= 'A' && character <= 'Z') {
            character = static_cast<char>(character + ('a' - 'A'));
        }
    }
    return text;
}

bool valid_nickname(const std::string &name) {
    if (name.empty() || name.size() > 24) return false;
    for (char character : name) {
        if (!((character >= 'a' && character <= 'z') ||
              (character >= 'A' && character <= 'Z') ||
              (character >= '0' && character <= '9') || character == '_')) {
            return false;
        }
    }
    return true;
}

struct Client {
    std::string input;
    std::string output;
    std::string nickname;
};

class Server {
public:
    Server(int port, const std::string &log_path)
        : log_(log_path, std::ios::app), listener_(socket(AF_INET, SOCK_STREAM, 0)),
          epoll_(epoll_create1(EPOLL_CLOEXEC)) {
        if (!log_) throw std::runtime_error("Cannot open log file: " + log_path);
        if (listener_ < 0 || epoll_ < 0) throw std::runtime_error("Cannot create sockets");

        int enabled = 1;
        setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
        nonblocking(listener_);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_ANY);
        address.sin_port = htons(static_cast<uint16_t>(port));
        if (bind(listener_, reinterpret_cast<sockaddr *>(&address), sizeof(address)) < 0 ||
            listen(listener_, SOMAXCONN) < 0) {
            throw std::runtime_error(std::string("Cannot bind/listen: ") + std::strerror(errno));
        }
        epoll_event event{};
        event.events = EPOLLIN;
        event.data.fd = listener_;
        if (epoll_ctl(epoll_, EPOLL_CTL_ADD, listener_, &event) < 0) {
            throw std::runtime_error("Cannot register listener with epoll");
        }
    }

    ~Server() {
        for (const auto &[fd, client] : clients_) {
            (void) client;
            close(fd);
        }
        if (listener_ >= 0) close(listener_);
        if (epoll_ >= 0) close(epoll_);
    }

    void run(int port) {
        std::cout << "Chat server listening on 0.0.0.0:" << port << '\n';
        note("Server started on port " + std::to_string(port));
        epoll_event ready[max_events];
        while (running) {
            const int count = epoll_wait(epoll_, ready, max_events, 1000);
            if (count < 0) {
                if (errno == EINTR) continue;
                throw std::runtime_error(std::string("epoll_wait: ") + std::strerror(errno));
            }
            for (int index = 0; index < count; ++index) {
                const int fd = ready[index].data.fd;
                if (fd == listener_) {
                    accept_clients();
                    continue;
                }
                if (!clients_.count(fd)) continue;
                const uint32_t events = ready[index].events;
                if (events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    disconnect(fd);
                    continue;
                }
                if (events & EPOLLIN) read_client(fd);
                if (clients_.count(fd) && (events & EPOLLOUT)) write_client(fd);
            }
        }
        note("Server stopped");
    }

private:
    void note(const std::string &text) {
        const std::time_t now = std::time(nullptr);
        const std::tm *utc = std::gmtime(&now);
        if (utc) log_ << std::put_time(utc, "%Y-%m-%dT%H:%M:%SZ") << ' ';
        log_ << text << '\n' << std::flush;
    }

    void update_interest(int fd) {
        auto it = clients_.find(fd);
        if (it == clients_.end()) return;
        epoll_event event{};
        event.events = EPOLLIN | EPOLLRDHUP;
        if (!it->second.output.empty()) event.events |= EPOLLOUT;
        event.data.fd = fd;
        if (epoll_ctl(epoll_, EPOLL_CTL_MOD, fd, &event) < 0) disconnect(fd);
    }

    void queue(int fd, const std::string &message) {
        auto it = clients_.find(fd);
        if (it == clients_.end()) return;
        if (it->second.output.size() + message.size() > max_pending_output) {
            note("Dropped slow client: " + it->second.nickname);
            disconnect(fd, false);
            return;
        }
        it->second.output += message;
        update_interest(fd);
    }

    void broadcast(const std::string &message, int except_fd = -1) {
        std::vector<int> recipients;
        recipients.reserve(clients_.size());
        for (const auto &[fd, client] : clients_) {
            if (fd != except_fd && !client.nickname.empty()) recipients.push_back(fd);
        }
        for (int fd : recipients) queue(fd, message);
    }

    void disconnect(int fd, bool announce = true) {
        auto it = clients_.find(fd);
        if (it == clients_.end()) return;
        std::string nickname = it->second.nickname;
        epoll_ctl(epoll_, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
        clients_.erase(it);
        if (!nickname.empty()) {
            note("Disconnected: " + nickname);
            if (announce) broadcast("* " + nickname + " left\n");
        }
    }

    void accept_clients() {
        for (;;) {
            sockaddr_in address{};
            socklen_t length = sizeof(address);
            int fd = accept(listener_, reinterpret_cast<sockaddr *>(&address), &length);
            if (fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                if (errno == EINTR) continue;
                note(std::string("accept failed: ") + std::strerror(errno));
                return;
            }
            try {
                nonblocking(fd);
                epoll_event event{};
                event.events = EPOLLIN | EPOLLRDHUP;
                event.data.fd = fd;
                if (epoll_ctl(epoll_, EPOLL_CTL_ADD, fd, &event) < 0) {
                    close(fd);
                    continue;
                }
                clients_.emplace(fd, Client{});
                queue(fd, "Welcome! Enter nickname (letters, numbers, underscore):\n");
            } catch (const std::exception &error) {
                note(error.what());
                close(fd);
            }
        }
    }

    int find_nickname(const std::string &name) const {
        for (const auto &[fd, client] : clients_) {
            if (!client.nickname.empty() && lower(client.nickname) == lower(name)) return fd;
        }
        return -1;
    }

    void process_line(int fd, const std::string &line) {
        auto it = clients_.find(fd);
        if (it == clients_.end()) return;
        if (it->second.nickname.empty()) {
            if (!valid_nickname(line)) {
                queue(fd, "Invalid nickname. Use 1-24 letters, digits or underscores.\n");
                return;
            }
            if (find_nickname(line) >= 0) {
                queue(fd, "Nickname is already in use.\n");
                return;
            }
            it->second.nickname = line;
            queue(fd, "Hello, " + line + "! Commands: /who, /msg <nick> <text>, /quit\n");
            broadcast("* " + line + " joined\n", fd);
            note("Connected: " + line);
            return;
        }
        const std::string nickname = it->second.nickname;
        if (line == "/quit") {
            disconnect(fd);
        } else if (line == "/who") {
            std::string response = "Online:";
            for (const auto &[other_fd, client] : clients_) {
                (void) other_fd;
                if (!client.nickname.empty()) response += " " + client.nickname;
            }
            queue(fd, response + "\n");
        } else if (line.rfind("/msg ", 0) == 0) {
            const std::size_t separator = line.find(' ', 5);
            if (separator == std::string::npos || separator + 1 == line.size()) {
                queue(fd, "Usage: /msg <nick> <text>\n");
                return;
            }
            const std::string target = line.substr(5, separator - 5);
            const int recipient = find_nickname(target);
            if (recipient < 0) {
                queue(fd, "User not found: " + target + "\n");
                return;
            }
            const std::string text = line.substr(separator + 1);
            queue(recipient, "[private from " + nickname + "] " + text + "\n");
            if (recipient != fd) queue(fd, "[private to " + target + "] " + text + "\n");
        } else if (!line.empty() && line[0] == '/') {
            queue(fd, "Unknown command. Use /who, /msg <nick> <text>, /quit\n");
        } else if (!line.empty()) {
            broadcast("[" + nickname + "] " + line + "\n");
        }
    }

    void read_client(int fd) {
        char buffer[4096];
        for (;;) {
            const ssize_t size = recv(fd, buffer, sizeof(buffer), 0);
            if (size == 0) {
                disconnect(fd);
                return;
            }
            if (size < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                disconnect(fd);
                return;
            }
            auto it = clients_.find(fd);
            if (it == clients_.end()) return;
            it->second.input.append(buffer, static_cast<std::size_t>(size));
            std::size_t end;
            while (clients_.count(fd) && (end = clients_.at(fd).input.find('\n')) != std::string::npos) {
                if (end > max_line) {
                    disconnect(fd);
                    return;
                }
                std::string line = clients_.at(fd).input.substr(0, end);
                clients_.at(fd).input.erase(0, end + 1);
                if (!line.empty() && line.back() == '\r') line.pop_back();
                process_line(fd, line);
            }
            if (!clients_.count(fd)) return;
            if (clients_.at(fd).input.size() > max_line) {
                disconnect(fd);
                return;
            }
        }
    }

    void write_client(int fd) {
        auto it = clients_.find(fd);
        while (it != clients_.end() && !it->second.output.empty()) {
            const ssize_t size = send(fd, it->second.output.data(), it->second.output.size(), MSG_NOSIGNAL);
            if (size < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                disconnect(fd);
                return;
            }
            it->second.output.erase(0, static_cast<std::size_t>(size));
        }
        update_interest(fd);
    }

    std::ofstream log_;
    int listener_;
    int epoll_;
    std::unordered_map<int, Client> clients_;
};
} // namespace

int main(int argc, char **argv) {
    try {
        int port = 9000;
        if (argc > 2) throw std::runtime_error("Usage: chat-server [port]");
        if (argc == 2) port = std::stoi(argv[1]);
        if (port < 1 || port > 65535) throw std::runtime_error("Port must be 1..65535");
        std::signal(SIGINT, stop);
        std::signal(SIGTERM, stop);
        std::signal(SIGPIPE, SIG_IGN);
        Server server(port, "chat-server.log");
        server.run(port);
    } catch (const std::exception &error) {
        std::cerr << "Error: " << error.what() << '\n';
        return 1;
    }
}