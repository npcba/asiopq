#pragma once

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <libpq-fe.h>

#include "error.hpp"

namespace ba {
namespace asiopq {

boost::asio::ip::tcp::socket dupTcpSocketFromHandle(boost::asio::io_service& ioService, boost::asio::ip::tcp::socket::native_handle_type handle)
{

#ifdef _WIN32
    ::WSAPROTOCOL_INFOW protoInfo;
    ::WSADuplicateSocketW(handle, ::GetCurrentProcessId(), &protoInfo);
    const auto dupHandle = ::WSASocketW(
          protoInfo.iAddressFamily
        , protoInfo.iSocketType
        , protoInfo.iProtocol
        , &protoInfo
        , 0
        , WSA_FLAG_OVERLAPPED
        );

    const auto family = protoInfo.iAddressFamily;

#else // POSIX
    const auto dupHandle = ::dup(handle);

    sockaddr_storage name;
    int nameLen = sizeof(name);
    const auto err = ::getsockname(dupHandle, reinterpret_cast<sockaddr*>(&name), &nameLen);
    const auto family = name.ss_family;
#endif

    return boost::asio::ip::tcp::socket{
          ioService
        , AF_INET6 == family ? boost::asio::ip::tcp::v6() : boost::asio::ip::tcp::v4()
        , dupHandle
    };
}

template <typename WaitHandler>
void asyncWaitReading(boost::asio::ip::tcp::socket& s, WaitHandler handler)
{
    s.async_read_some(boost::asio::null_buffers{}, std::bind(std::move(handler), std::placeholders::_1));
}

template <typename WaitHandler>
void asyncWaitWriting(boost::asio::ip::tcp::socket& s, WaitHandler handler)
{
    s.async_write_some(boost::asio::null_buffers{}, std::bind(std::move(handler), std::placeholders::_1));
}

template <typename ConnectHandler>
class ConnectOp
{
public:
    ConnectOp(ConnectOp&&) = default;
    ConnectOp& operator=(ConnectOp&&) = default;

    ConnectOp(const ConnectOp&) = default; // старый boost требует копирования от handler
    ConnectOp& operator=(const ConnectOp&) = default;

    ConnectOp(PGconn* conn, boost::asio::ip::tcp::socket& s, ConnectHandler handler)
        : m_conn{ conn }
        , m_socket{ s }
        , m_handler{ std::move(handler) }
    {
    }

    void operator()(const boost::system::error_code& ec)
    {
        if (ec)
        {
            m_handler(ec);
            return;
        }

        const auto pollResult = ::PQconnectPoll(m_conn);
        switch (pollResult)
        {
        case PGRES_POLLING_OK:
            m_handler(boost::system::error_code{});
            return;
        case PGRES_POLLING_FAILED:
            m_handler(make_error_code(PQError::CONNECT_POLL_FAILED));
            return;
        case PGRES_POLLING_READING:
            asyncWaitReading(m_socket, std::move(*this));
            return;
        case PGRES_POLLING_WRITING:
            asyncWaitWriting(m_socket, std::move(*this));
            return;
        default:
            break;
        }
    }

private:
    PGconn* m_conn;
    boost::asio::ip::tcp::socket& m_socket;
    ConnectHandler m_handler;
};

template <typename ExecHandler>
class ExecOp
{
public:
    ExecOp(ExecOp&&) = default;
    ExecOp& operator=(ExecOp&&) = default;

    ExecOp(const ExecOp&) = default; // старый boost требует копирования от handler
    ExecOp& operator=(const ExecOp&) = default;

    ExecOp(PGconn* conn, boost::asio::ip::tcp::socket& s, ExecHandler handler)
        : m_conn{ conn }
        , m_socket{ s }
        , m_handler{ std::move(handler) }
    {
    }

    void operator()(const boost::system::error_code& ec)
    {
        if (ec)
        {
            m_handler(ec, nullptr);
            return;
        }

        if (!::PQconsumeInput(m_conn))
        {
        }

        while (true)
        {
            if (::PQisBusy(m_conn))
            {
                asyncWaitReading(m_socket, std::move(*this));
                return;
            }

            ::PGresult* res = ::PQgetResult(m_conn);
            m_handler(boost::system::error_code{}, res);

            if (!res)
                return;

            ::PQclear(res);
        }
    }

private:
    PGconn* m_conn;
    boost::asio::ip::tcp::socket& m_socket;
    ExecHandler m_handler;
};

class Connection
{
public:
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    explicit Connection(boost::asio::io_service& ioService)
        : m_socket{ std::make_unique<boost::asio::ip::tcp::socket>(ioService) }
    {
    }

    ~Connection() noexcept
    {
        close();
    }

    Connection(Connection&& other) noexcept
        : m_socket{ std::move(other.m_socket) }
        , m_conn{ other.m_conn }
    {
        other.m_conn = nullptr;
    }

    Connection& operator=(Connection&& other) noexcept
    {
        m_socket = std::move(other.m_socket);
        m_conn = other.m_conn;
        other.m_conn = nullptr;
    }

    PGconn* get() const noexcept
    {
        return m_conn;
    }

    template <typename ConnectHandler>
    void asyncConnect(const char* conninfo, ConnectHandler handler)
    {
        close();

        m_conn = ::PQconnectStart(conninfo);
        if (!m_conn)
            return;

        *m_socket = dupTcpSocketFromHandle(m_socket->get_io_service(), ::PQsocket(m_conn));
        m_socket->non_blocking(true);

        using trueH = boost::asio::handler_type<ConnectHandler, void(boost::system::error_code)>::type;
        trueH h{ std::move(handler) };
        boost::asio::async_result<trueH> res{ h };

        ConnectOp<trueH> connOp{ m_conn, *m_socket, std::move(h) };
        m_socket->get_io_service().post(std::bind(std::move(connOp), boost::system::error_code{}));
        res.get();
    }

    template <typename Command, typename ExecHandler>
    void asyncExec(Command&& cmd, ExecHandler handler)
    {
        if (!cmd())
        {
        }

        ExecOp<ExecHandler> exOp{ m_conn, *m_socket, std::move(handler) };
        m_socket->get_io_service().post(std::bind(std::move(exOp), boost::system::error_code{}));
    }

    void close() noexcept
    {
        if (m_socket->is_open())
        {
            boost::system::error_code ec;
            m_socket->close(ec);
        }

        if (m_conn)
        {
            ::PQfinish(m_conn);
            m_conn = nullptr;
        }
    }

private:
    std::unique_ptr<boost::asio::ip::tcp::socket> m_socket;
    PGconn* m_conn = nullptr;
};

} // asiopq
} // namespace ba
