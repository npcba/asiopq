#pragma once

#include <libpq-fe.h>

#include <boost/asio/async_result.hpp>

#include "detail/dup_socket.hpp"
#include "detail/operations.hpp"
#include "ignore_result.hpp"


namespace ba {
namespace asiopq {

using boost::asio::handler_type; // fix for wrong BOOST_ASIO_HANDLER_TYPE impl

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
        : m_conn{ other.m_conn }
        , m_socket{ std::move(other.m_socket) }
    {
        other.m_conn = nullptr;
    }

    Connection& operator=(Connection&& other) noexcept
    {
        m_conn = other.m_conn;
        other.m_conn = nullptr;
        m_socket = std::move(other.m_socket);
    }

    PGconn* get() noexcept
    {
        return m_conn;
    }

    const PGconn* get() const noexcept
    {
        return m_conn;
    }

    template <typename ConnectHandler>
    auto asyncConnect(const char* conninfo, ConnectHandler&& handler)
    {
        // If you get an error on the following line it means that your handler does
        // not meet the documented type requirements for a ConnectHandler.
        BOOST_ASIO_CONNECT_HANDLER_CHECK(ConnectHandler, handler) type_check;

        /*if (::CONNECTION_BAD != ::PQstatus(m_conn))
        {
            return;??
        }*/

        m_conn = ::PQconnectStart(conninfo);
        return startConnectPoll(std::forward<ConnectHandler>(handler));
    }

    template <typename ConnectHandler>
    void asyncConnectParams(const char* const* keywords, const char* const* values, int expandDbname, ConnectHandler&& handler)
    {
        // If you get an error on the following line it means that your handler does
        // not meet the documented type requirements for a ConnectHandler.
        BOOST_ASIO_CONNECT_HANDLER_CHECK(ConnectHandler, handler) type_check;

        /*if (::CONNECTION_BAD != ::PQstatus(m_conn))
        {
            return;??
        }*/

        m_conn = ::PQconnectStartParams(keywords, values, expandDbname);
        return startConnectPoll(std::forward<ConnectHandler>(handler));
    }

    template <typename SendCmd, typename ExecHandler, typename ResultCollector = IgnoreResult>
    auto asyncExec(SendCmd&& cmd, ExecHandler&& handler, ResultCollector&& coll = {})
    {
        // If you get an error on the following line it means that your handler does
        // not meet the documented type requirements for a ExecHandler.
        BOOST_ASIO_ACCEPT_HANDLER_CHECK(ExecHandler, handler) type_check;

        boost::asio::detail::async_result_init<ExecHandler, void(boost::system::error_code)>
            init{ std::forward<ExecHandler>(handler) };

        boost::system::error_code ec = cmd();

        using ExecOpType = detail::ExecOp<decltype(init.handler), ResultCollector>;
        m_socket->get_io_service().post(
            [handler{ ExecOpType{ m_conn, *m_socket, std::move(init.handler), std::forward<ResultCollector>(coll) } }, ec{ ec }]() mutable {
            handler(ec);
        });

        return init.result.get();
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
    auto startConnectPoll(ConnectHandler&& handler)
    {
        boost::asio::detail::async_result_init<ConnectHandler, void(boost::system::error_code)>
            init{ std::forward<ConnectHandler>(handler) };

        boost::system::error_code ec;
        int nativeSocket = -1;

        if (m_conn)
            if (-1 != (nativeSocket = ::PQsocket(m_conn)))
                if (!(ec = detail::dupTcpSocketFromHandle(nativeSocket, *m_socket)))
                    ec = m_socket->non_blocking(true, ec);
            else
                ec = make_error_code(PQError::CONN_INVALID_SOCKET);
        else
            ec = make_error_code(PQError::CONN_ALLOC_FAILED);

        using ConnOpType = detail::ConnectOp<decltype(init.handler)>;
        m_socket->get_io_service().post(
            [handler{ ConnOpType{ m_conn, *m_socket, std::move(init.handler) } }, ec] () mutable {
            handler(ec);
        });

        return init.result.get();
    }

private:
    PGconn* m_conn = nullptr;
    std::unique_ptr<boost::asio::ip::tcp::socket> m_socket;
};

} // namespace asiopq
} // namespace ba
