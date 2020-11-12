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
    // TS: s.async_wait(boost::asio::ip::tcp::socket::wait_read, ...);
}

template <typename WaitHandler>
void asyncWaitWriting(boost::asio::ip::tcp::socket& s, WaitHandler handler)
{
    s.async_write_some(boost::asio::null_buffers{}, std::bind(std::move(handler), std::placeholders::_1));
    // TS: s.async_wait(boost::asio::ip::tcp::socket::wait_write, ...);
}

template <typename CompletionHandler>
class OperationBase
{
public:
    OperationBase(PGconn* conn, boost::asio::ip::tcp::socket& s, CompletionHandler&& handler)
        : m_conn{ conn }
        , m_socket{ s }
        , m_handler{ std::forward<CompletionHandler>(handler) }
    {
    }

protected:
    void invokeHandler(const boost::system::error_code& ec)
    {
        boost::asio::detail::binder1<CompletionHandler, boost::system::error_code> binder{ m_handler, ec };
        boost_asio_handler_invoke_helpers::invoke(binder, binder.handler_);
    }

protected:
    PGconn* m_conn;
    boost::asio::ip::tcp::socket& m_socket;
    CompletionHandler m_handler;
};

template <typename CompletionHandler>
class ConnectOp
    : private OperationBase<CompletionHandler>
{
public:
    ConnectOp(ConnectOp&&) = default;
    ConnectOp& operator=(ConnectOp&&) = default;

    ConnectOp(const ConnectOp&) = default; // старый boost требует копирования от handler
    ConnectOp& operator=(const ConnectOp&) = default;

    ConnectOp(PGconn* conn, boost::asio::ip::tcp::socket& s, CompletionHandler&& handler)
        : OperationBase<CompletionHandler>{conn, s, std::forward<CompletionHandler>(handler)}
    {
    }

    void operator()(const boost::system::error_code& ec)
    {
        if (ec)
        {
            invokeHandler(ec);
            return;
        }

        const auto pollResult = ::PQconnectPoll(m_conn);
        switch (pollResult)
        {
        case PGRES_POLLING_OK:
        {
            const ::ConnStatusType status = ::PQstatus(m_conn);
            if (::CONNECTION_OK != status)
                return;

            invokeHandler(boost::system::error_code{});
            return;
        }
        case PGRES_POLLING_FAILED:
            invokeHandler(make_error_code(PQError::CONNECT_POLL_FAILED));
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
};

template <typename CompletionHandler, typename ResultCollector>
class ExecOp
    : private OperationBase<CompletionHandler>
{
public:
    ExecOp(ExecOp&&) = default;
    ExecOp& operator=(ExecOp&&) = default;

    ExecOp(const ExecOp&) = default; // старый boost требует копирования от handler
    ExecOp& operator=(const ExecOp&) = default;

    ExecOp(PGconn* conn, boost::asio::ip::tcp::socket& s, CompletionHandler&& handler, ResultCollector&& coll)
        : OperationBase<CompletionHandler>{ conn, s, std::forward<CompletionHandler>(handler) }
        , m_collector{ std::forward<ResultCollector>(coll) }
    {
    }

    void operator()(const boost::system::error_code& ec)
    {
        if (ec)
        {
            invokeHandler(ec);
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
            m_collector(res);

            if (!res) // все данные обработаны
            {
                invokeHandler(boost::system::error_code{});
                return;
            }

            ::PQclear(res);
        }
    }

private:
    ResultCollector m_collector;
};

struct IgnoreResult
{
    void operator()(::PGresult* res) const noexcept
    {
    }
};

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

        auto nativeSocket = ::PQsocket(m_conn);
        if (-1 == nativeSocket)
            return;

        *m_socket = dupTcpSocketFromHandle(m_socket->get_io_service(), nativeSocket);
        m_socket->non_blocking(true);

        boost::asio::detail::async_result_init<ConnectHandler, void(boost::system::error_code)>
            init{ std::forward<ConnectHandler>(handler) };

        ConnectOp<decltype(init.handler)> connOp{ m_conn, *m_socket, std::move(init.handler) };
        m_socket->get_io_service().post(std::bind(std::move(connOp), boost::system::error_code{}));
        init.result.get();
    }

    template <typename SendCmd, typename ExecHandler, typename ResultCollector = IgnoreResult>
    void asyncExec(SendCmd&& cmd, ExecHandler&& handler, ResultCollector&& coll = IgnoreResult{})
    {
        if (!cmd())
        {
        }

        boost::asio::detail::async_result_init<ExecHandler, void(boost::system::error_code)>
            init{ std::forward<ExecHandler>(handler) };

        ExecOp<decltype(init.handler), ResultCollector> exOp{ m_conn, *m_socket, std::move(init.handler), std::forward<ResultCollector>(coll) };
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
    std::unique_ptr<boost::asio::ip::tcp::socket> m_socket;
    PGconn* m_conn = nullptr;
};

template <typename Handler, typename ResultCollector = IgnoreResult>
boost::system::error_code asyncQuery(Connection& conn, const char* query, Handler&& handler, ResultCollector&& coll = IgnoreResult{})
{
    conn.asyncExec(
        [pgConn{ conn.get() }, query] {
            if (!::PQsendQuery(pgConn, query))
            {
            }
            return boost::system::error_code{};
        },
        std::forward<Handler>(handler),
        std::forward<ResultCollector>(coll)
    );

    return {};
}

template <typename Handler, typename ResultCollector = IgnoreResult>
boost::system::error_code asyncPrepare(Connection& conn, const char* name, const char* query, Handler&& handler, ResultCollector&& coll = IgnoreResult{})
{
    conn.asyncExec(
        [pgConn{ conn.get() }, name, query]{
            if (!::PQsendPrepare(pgConn, name, query, 0, nullptr))
            {
            }
            return boost::system::error_code{};
        },
        std::forward<Handler>(handler),
        std::forward<ResultCollector>(coll)
    );

    return {};
}

template <typename Handler, typename ResultCollector = IgnoreResult>
boost::system::error_code asyncQueryPrepared(Connection& conn, const char* name, Handler&& handler, ResultCollector&& coll = IgnoreResult{})
{
    conn.asyncExec(
        [pgConn{ conn.get() }, name]{
            if (!::PQsendQueryPrepared(pgConn, name, 0, nullptr, nullptr, nullptr, 0))
            {
            }
            return boost::system::error_code{};
        },
        std::forward<Handler>(handler),
        std::forward<ResultCollector>(coll)
    );

    return {};
}

class PreparedQuery
{
    PreparedQuery(const PreparedQuery&) = delete;
    PreparedQuery& operator=(const PreparedQuery&) = delete;

public:
    PreparedQuery(Connection& conn, std::string query)
        : m_conn{ conn }
        , m_query{ std::move(query) }
        , m_name{ generateUniqueName() }
    {
    }

    template <typename Handler>
    void operator()(Handler&& handler)
    {
        if (m_prepared)
        {
            asyncQueryPrepared(m_conn, m_name.c_str(), std::forward<Handler>(handler));
            return;
        }

        boost::asio::detail::async_result_init<Handler, void(boost::system::error_code)> init{ std::forward<Handler>(handler) };
        auto hidden = [h{ std::move(init.handler) }](const boost::system::error_code& ec) mutable {
            boost::asio::detail::binder1<decltype(h), boost::system::error_code> binder{ std::move(h), ec };
            boost_asio_handler_invoke_helpers::invoke(binder, binder.handler_);
        };

        asyncPrepare(m_conn, m_name.c_str(), m_query.c_str(), [this, h{ std::move(hidden) }](const boost::system::error_code& ec) mutable {
            if (ec)
            {
            }

            m_prepared = true;
            asyncQueryPrepared(m_conn, m_name.c_str(), std::move(h));
        });

        init.result.get();
    }

private:
    static std::string generateUniqueName()
    {
        static std::atomic_uint uniqueN = 0;
        return std::to_string(++uniqueN);
    }

private:
    Connection& m_conn;
    const std::string m_query;
    const std::string m_name;
    bool m_prepared = false;
};

} // asiopq
} // namespace ba
