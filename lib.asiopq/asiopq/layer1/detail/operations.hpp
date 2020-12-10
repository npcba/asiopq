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
        boost::asio::detail::binder1<CompletionHandler, boost::system::error_code> binder{ std::move(m_handler), ec };
        boost_asio_handler_invoke_helpers::invoke(binder, binder.handler_);
    }

protected:
    PGconn* m_conn;
    boost::asio::ip::tcp::socket& m_socket;
    CompletionHandler m_handler;
};

template <typename ConnectHandler>
class ConnectOp
    : private OperationBase<ConnectHandler>
{
    using Base = OperationBase<ConnectHandler>;

public:
    ConnectOp(ConnectOp&&) = default;
    ConnectOp& operator=(ConnectOp&&) = default;

    // старый boost требует копирования от handler,
    // для нового нужно будет запретить копирование, оствить только move для гарантии, что нет копирования
    ConnectOp(const ConnectOp&) = default;
    ConnectOp& operator=(const ConnectOp&) = default;

    ConnectOp(PGconn* conn, boost::asio::ip::tcp::socket& s, ConnectHandler&& handler)
        : Base{ conn, s, std::forward<ConnectHandler>(handler) }
    {
    }

    void operator()(const boost::system::error_code&)
    {
        // пусть PQconnectPoll сам обработает ошибку
        //if (ec)
        //    return Base::invokeHandler(ec);

        const auto pollResult = ::PQconnectPoll(Base::m_conn);
        switch (pollResult)
        {
        case PGRES_POLLING_OK:
        {
            assert(::PQstatus(Base::m_conn) == ::CONNECTION_OK);
            return Base::invokeHandler(boost::system::error_code{});
        }
        case PGRES_POLLING_READING:
            return detail::asyncWaitReading(Base::m_socket, std::move(*this));
        case PGRES_POLLING_WRITING:
            return detail::asyncWaitWriting(Base::m_socket, std::move(*this));
        default:
            return Base::invokeHandler(make_error_code(PQError::CONN_POLL_FAILED));
        }
    }
};

template <typename ExecHandler, typename ResultCollector>
class ExecOp
    : private OperationBase<ExecHandler>
{
    using Base = OperationBase<ExecHandler>;

public:
    ExecOp(ExecOp&&) = default;
    ExecOp& operator=(ExecOp&&) = default;

    // старый boost требует копирования от handler,
    // для нового нужно будет запретить копирование, оствить только move для гарантии, что нет копирования
    ExecOp(const ExecOp&) = default;
    ExecOp& operator=(const ExecOp&) = default;

    ExecOp(PGconn* conn, boost::asio::ip::tcp::socket& s, ExecHandler&& handler, ResultCollector&& coll)
        : Base{ conn, s, std::forward<ExecHandler>(handler) }
        , m_collector{ std::forward<ResultCollector>(coll) }
    {
    }

    void operator()(const boost::system::error_code&)
    {
        // пусть PQconsumeInput сам обработает ошибку
        //if (ec)
        //    return Base::invokeHandler(ec);

        switch (const bool JUMP_OVER_FIRST_CHECK = {})
        {
            for (;;)
            {
                if (::PQisBusy(Base::m_conn)) // если команда еще в процессе
                {
        case JUMP_OVER_FIRST_CHECK:
                    if (!::PQconsumeInput(Base::m_conn)) // пробуем забрть из сокета все, что накопилось без блокирования
                        return Base::invokeHandler(make_error_code(PQError::CONSUME_INPUT_FAILED));

                    if (::PQisBusy(Base::m_conn)) // опять проверяем, может получили необходимые данные
                        return detail::asyncWaitReading(Base::m_socket, std::move(*this)); // не получили, уходим в ожидание сокета на чтение
                }

                ::PGresult* res = ::PQgetResult(Base::m_conn);
                const auto curEc = m_collector(res);
                if (curEc)
                    m_lastEc = curEc; // если ошибка, то сохраняем ее (перезаписываем предыдущую)

                if (!res) // nullptr означает конец обработки данных (согласно документации PQgetResult)
                    return Base::invokeHandler(m_lastEc);

                ::PQclear(res);
            }
        }
    }

private:
    ResultCollector m_collector;
    boost::system::error_code m_lastEc; // последняя ошибка, которую вернул m_collector
};

} // namespace detail
} // namespace asiopq
} // namespace ba
