#pragma once

#include <array>
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
    socklen_t nameLen = sizeof(name);
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
            this->invokeHandler(ec);
            return;
        }

        const auto pollResult = ::PQconnectPoll(this->m_conn);
        switch (pollResult)
        {
        case PGRES_POLLING_OK:
        {
            const ::ConnStatusType status = ::PQstatus(this->m_conn);
            if (::CONNECTION_OK != status)
                return;

            this->invokeHandler(boost::system::error_code{});
            return;
        }
        case PGRES_POLLING_FAILED:
            this->invokeHandler(make_error_code(PQError::CONNECT_POLL_FAILED));
            return;
        case PGRES_POLLING_READING:
            asyncWaitReading(this->m_socket, std::move(*this));
            return;
        case PGRES_POLLING_WRITING:
            asyncWaitWriting(this->m_socket, std::move(*this));
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
            this->invokeHandler(ec);
            return;
        }

        if (!::PQconsumeInput(this->m_conn))
        {
        }

        while (true)
        {
            if (::PQisBusy(this->m_conn))
            {
                asyncWaitReading(this->m_socket, std::move(*this));
                return;
            }

            ::PGresult* res = ::PQgetResult(this->m_conn);
            m_collector(res);

            if (!res) // все данные обработаны
            {
                this->invokeHandler(boost::system::error_code{});
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
        /*::ExecStatusType status = ::PQresultStatus(res);
        PQprintOpt opt = { 0 };
        opt.header = 1;
        opt.align = 1;
        //opt.expanded = 1;
        opt.fieldSep = ", ";
        ::PQprint(stdout, res, &opt);*/
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
    template <typename ConnectHandler>
    void startConnectPoll(ConnectHandler&& handler)
    {
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

private:
    std::unique_ptr<boost::asio::ip::tcp::socket> m_socket;
    PGconn* m_conn = nullptr;
};

class NullParams
{
public:
    constexpr int n() const noexcept
    {
        return 0;
    }

    constexpr const Oid* types() const noexcept
    {
        return nullptr;
    }

    constexpr const char* const* values() const noexcept
    {
        return nullptr;
    }

    constexpr const int* lengths() const noexcept
    {
        return nullptr;
    }

    constexpr const int* formats() const noexcept
    {
        return nullptr;
    }
};

template <std::size_t length>
class TextParams
{
public:
    template <typename... Char>
    TextParams(const Char*... params...) noexcept
        : m_params{ checkedChar(params)... }
    {
        static_assert(sizeof...(params) == length, "Constructor argument count should be equal to 'length' template parameter");
    }

    TextParams(const char* const(&params)[length]) noexcept
    {
        std::copy(std::begin(params), std::end(params), m_params.begin());
    }

    constexpr int n() const noexcept
    {
        return int(m_params.size());
    }

    constexpr const Oid* types() const noexcept
    {
        return nullptr;
    }

    const char* const* values() const noexcept
    {
        return m_params.data();
    }

    constexpr const int* lengths() const noexcept
    {
        return nullptr;
    }

    constexpr const int* formats() const noexcept
    {
        return nullptr;
    }

private:
    template <typename Char>
    static constexpr const char* checkedChar(const Char* param) noexcept
    {
        static_assert(std::is_same_v<Char, char>, "Only const char* parameters are allowed");
        return param;
    }

private:
    std::array<const char*, length> m_params;
};

template <typename... Char>
TextParams<sizeof...(Char)> makeTextParams(const Char*... params...)
{
    return { params... };
}

template <std::size_t length>
TextParams<length> makeTextParams(const char* const(&params)[length])
{
    return { params };
}

template <typename Handler, typename ResultCollector = IgnoreResult>
boost::system::error_code asyncQuery(Connection& conn, const char* query, Handler&& handler, ResultCollector&& coll = {})
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

template <typename Params, typename Handler, typename ResultCollector = IgnoreResult>
boost::system::error_code asyncQueryParams(Connection& conn, const char* command, const Params& params, bool textResultFormat, Handler&& handler, ResultCollector&& coll = {})
{
    conn.asyncExec(
        [pgConn{ conn.get() }, command, &params, textResultFormat]{
            if (!::PQsendQueryParams(pgConn, command, params.n(), params.types(), params.values(), params.lengths(), params.formats(), textResultFormat ? 0 : 1))
            {
            }
            return boost::system::error_code{};
        },
        std::forward<Handler>(handler),
        std::forward<ResultCollector>(coll)
    );

    return {};
}

template <typename Params, typename Handler, typename ResultCollector = IgnoreResult>
boost::system::error_code asyncPrepareParams(Connection& conn, const char* name, const char* query, const Params& params, Handler&& handler, ResultCollector&& coll = {})
{
    conn.asyncExec(
        [pgConn{ conn.get() }, name, query, &params]{
            if (!::PQsendPrepare(pgConn, name, query, params.n(), params.types()))
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
boost::system::error_code asyncPrepare(Connection& conn, const char* name, const char* query, Handler&& handler, ResultCollector&& coll = {})
{
    return asyncPrepareParams(conn, name, query, NullParams{}, std::forward<Handler>(handler), std::forward<ResultCollector>(coll));
}

template <typename Params, typename Handler, typename ResultCollector = IgnoreResult>
boost::system::error_code asyncQueryPrepared(Connection& conn, const char* name, const Params& params, bool textResultFormat, Handler&& handler, ResultCollector&& coll = {})
{
    conn.asyncExec(
        [pgConn{ conn.get() }, name, &params, textResultFormat]{
            if (!::PQsendQueryPrepared(pgConn, name, params.n(), params.values(), params.lengths(), params.formats(), textResultFormat ? 0 : 1))
            {
            }
            return boost::system::error_code{};
        },
        std::forward<Handler>(handler),
        std::forward<ResultCollector>(coll)
    );

    return {};
}

template <typename PrepareParams = NullParams>
class PreparedQuery
{
    PreparedQuery(const PreparedQuery&) = delete;
    PreparedQuery& operator=(const PreparedQuery&) = delete;

public:
    PreparedQuery(Connection& conn, std::string query, bool textResultFormat = true, PrepareParams prepareParams = {})
        : m_conn{ conn }
        , m_query{ std::move(query) }
        , m_prepareParams{ std::move(prepareParams) }
        , m_name{ generateUniqueName() }
        , m_textResultFormat{ textResultFormat }
    {
    }

    template <typename Params, typename Handler>
    void operator()(Params&& params, Handler&& handler)
    {
        if (m_prepared)
        {
            asyncQueryPrepared(m_conn, m_name.c_str(), std::forward<Params>(params), m_textResultFormat, std::forward<Handler>(handler));
            return;
        }

        boost::asio::detail::async_result_init<Handler, void(boost::system::error_code)> init{ std::forward<Handler>(handler) };
        auto hidden = [h{ std::move(init.handler) }](const boost::system::error_code& ec) mutable {
            boost::asio::detail::binder1<decltype(h), boost::system::error_code> binder{ std::move(h), ec };
            boost_asio_handler_invoke_helpers::invoke(binder, binder.handler_);
        };

        asyncPrepareParams(m_conn, m_name.c_str(), m_query.c_str(), m_prepareParams, [this, params{ std::forward<Params>(params) }, h{ std::move(hidden) }](const boost::system::error_code& ec) mutable {
            if (ec)
            {
            }

            m_prepared = true;
            asyncQueryPrepared(m_conn, m_name.c_str(), std::move(params), m_textResultFormat, std::move(h));
        });

        init.result.get();
    }

private:
    static std::string generateUniqueName()
    {
        static std::atomic_uint uniqueN;
        return std::to_string(++uniqueN);
    }

private:
    Connection& m_conn;
    const std::string m_query;
    const PrepareParams m_prepareParams;
    const std::string m_name;
    const bool m_textResultFormat = true;
    bool m_prepared = false;
};

} // asiopq
} // namespace ba
