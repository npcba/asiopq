#pragma once

#include <libpq-fe.h>

#include <boost/asio/async_result.hpp>

#include "detail/dup_socket.hpp"
#include "detail/operations.hpp"
#include "ignore_result.hpp"

namespace ba {
namespace asiopq {

class Connection
{
public:
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    explicit Connection(boost::asio::io_service& ios)
        : m_socket{ std::make_unique<boost::asio::ip::tcp::socket>(ios) }
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

    PGconn* get() noexcept
    {
        return m_conn;
    }

    template <typename ConnectHandler>
    void asyncConnect(const char* conninfo, ConnectHandler&& handler)
    {
        if (::CONNECTION_BAD != ::PQstatus(m_conn))
        {
            return;
        }

        m_conn = ::PQconnectStart(conninfo);
        if (!m_conn)
            return;

        return startConnectPoll(std::forward<ConnectHandler>(handler));
    }

    template <typename ConnectHandler>
    void asyncConnectParams(const char* const* keywords, const char* const* values, int expandDbname, ConnectHandler&& handler)
    {
        if (::CONNECTION_BAD != ::PQstatus(m_conn))
        {
            return;
        }

        m_conn = ::PQconnectStartParams(keywords, values, expandDbname);
        if (!m_conn)
            return;

        return startConnectPoll(std::forward<ConnectHandler>(handler));
    }

    template <typename SendCmd, typename ExecHandler, typename ResultCollector = IgnoreResult>
    void asyncExec(SendCmd&& cmd, ExecHandler&& handler, ResultCollector&& coll = {})
    {
        if (!cmd())
        {
        }

        boost::asio::detail::async_result_init<ExecHandler, void(boost::system::error_code)>
            init{ std::forward<ExecHandler>(handler) };

        detail::ExecOp<decltype(init.handler), ResultCollector> exOp{ m_conn, *m_socket, std::move(init.handler), std::forward<ResultCollector>(coll) };
        m_socket->get_io_service().post(std::bind(std::move(exOp), boost::system::error_code{}));
        init.result.get();
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
    template <typename ConnectHandler>
    void startConnectPoll(ConnectHandler&& handler)
    {
        auto nativeSocket = ::PQsocket(m_conn);
        if (-1 == nativeSocket)
            return;

        *m_socket = detail::dupTcpSocketFromHandle(m_socket->get_io_service(), nativeSocket);
        m_socket->non_blocking(true);

        boost::asio::detail::async_result_init<ConnectHandler, void(boost::system::error_code)>
            init{ std::forward<ConnectHandler>(handler) };

        detail::ConnectOp<decltype(init.handler)> connOp{ m_conn, *m_socket, std::move(init.handler) };
        m_socket->get_io_service().post(std::bind(std::move(connOp), boost::system::error_code{}));
        init.result.get();
    }

private:
    std::unique_ptr<boost::asio::ip::tcp::socket> m_socket;
    PGconn* m_conn = nullptr;
};

} // namespace asiopq
} // namespace ba
