#pragma once

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/detail/bind_handler.hpp>
#include <boost/asio/detail/handler_invoke_helpers.hpp>

#include <libpq-fe.h>

#include "async_wait_socket.hpp"
#include "../../error.hpp"

namespace ba {
namespace asiopq {
namespace detail {

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
    using Base = OperationBase<CompletionHandler>;

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
            Base::invokeHandler(ec);
            return;
        }

        const auto pollResult = ::PQconnectPoll(Base::m_conn);
        switch (pollResult)
        {
        case PGRES_POLLING_OK:
        {
            const ::ConnStatusType status = ::PQstatus(Base::m_conn);
            if (::CONNECTION_OK != status)
                return;

            Base::invokeHandler(boost::system::error_code{});
            return;
        }
        case PGRES_POLLING_FAILED:
            Base::invokeHandler(make_error_code(PQError::CONNECT_POLL_FAILED));
            return;
        case PGRES_POLLING_READING:
            detail::asyncWaitReading(Base::m_socket, std::move(*this));
            return;
        case PGRES_POLLING_WRITING:
            detail::asyncWaitWriting(Base::m_socket, std::move(*this));
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
    using Base = OperationBase<CompletionHandler>;

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
            Base::invokeHandler(ec);
            return;
        }

        if (!::PQconsumeInput(Base::m_conn))
        {
        }

        while (true)
        {
            if (::PQisBusy(Base::m_conn))
            {
                detail::asyncWaitReading(Base::m_socket, std::move(*this));
                return;
            }

            ::PGresult* res = ::PQgetResult(Base::m_conn);
            m_collector(res);

            if (!res) // все данные обработаны
            {
                Base::invokeHandler(boost::system::error_code{});
                return;
            }

            ::PQclear(res);
        }
    }

private:
    ResultCollector m_collector;
};

} // namespace detail
} // namespace asiopq
} // namespace ba
