#pragma once

#include <memory>

#include <libpq-fe.h>

#include <boost/asio/async_result.hpp>

#include "detail/dup_socket.hpp"
#include "detail/operations.hpp"
#include "ignore_result.hpp"

namespace boost {
namespace asio {

template <typename Handler>
void ba_asiopq_handlerCheck(Handler&& handler)
{
    // If you get an error on the following line it means that your handler does
    // not meet the documented type requirements for a Handler:
    // void handler(
    //     const boost::system::error_code & ec // Result of operation
    // );
    BOOST_ASIO_CONNECT_HANDLER_CHECK(Handler, handler) type_check;
}

} // namespace asio
} // namespace boost

namespace ba {
namespace asiopq {

class Connection
{
public:
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    explicit Connection(boost::asio::io_service& ios)
        : m_conn{ nullptr, ::PQfinish }
        , m_socket{ std::make_unique<boost::asio::ip::tcp::socket>(ios) }
    {
    }

    ~Connection() noexcept
    {
        close();
    }

    Connection(Connection&& other) = default;
    Connection& operator=(Connection&& other) = default;

    PGconn* get() noexcept
    {
        return m_conn.get();
    }

    const PGconn* get() const noexcept
    {
        return m_conn.get();
    }

    template <typename ConnectHandler>
    auto asyncConnect(const char* conninfo, ConnectHandler&& handler)
    {
        // If you get an error on the following line it means that your handler does
        // not meet the documented type requirements for a ConnectHandler:
        // void handler(
        //     const boost::system::error_code & ec // Result of operation
        // );
        boost::asio::ba_asiopq_handlerCheck(handler);

        /*if (::CONNECTION_BAD != ::PQstatus(m_conn))
        {
            return;?? оказывается libpq сама проверяет
        }*/

        m_conn.reset(::PQconnectStart(conninfo));
        return startConnectPoll(std::forward<ConnectHandler>(handler));
    }

    template <typename ConnectHandler>
    auto asyncConnectParams(const char* const* keywords, const char* const* values,
                            int expandDbname, ConnectHandler&& handler)
    {
        // If you get an error on the following line it means that your handler does
        // not meet the documented type requirements for a ConnectHandler:
        // void handler(
        //     const boost::system::error_code & ec // Result of operation
        // );
        boost::asio::ba_asiopq_handlerCheck(handler);

        /*if (::CONNECTION_BAD != ::PQstatus(m_conn))
        {
            return;?? оказывается libpq сама проверяет
        }*/

        m_conn.reset(::PQconnectStartParams(keywords, values, expandDbname));
        return startConnectPoll(std::forward<ConnectHandler>(handler));
    }

    template <typename SendCmd, typename ExecHandler, typename ResultCollector = IgnoreResult>
    auto asyncExec(SendCmd&& cmd, ExecHandler&& handler, ResultCollector&& coll = {})
    {
        // If you get an error on the following line it means that your handler does
        // not meet the documented type requirements for a ConnectHandler:
        // void handler(
        //     const boost::system::error_code & ec // Result of operation
        // );
        boost::asio::ba_asiopq_handlerCheck(handler);

        boost::asio::detail::async_result_init<ExecHandler, void(boost::system::error_code)>
            init{ std::forward<ExecHandler>(handler) };

        boost::system::error_code ec = cmd();

        using ExecOpType = detail::ExecOp<decltype(init.handler), ResultCollector>;
        // здесь разделено на 2 разных вызова: в случае ошибки ec уходит в капчу,
        // а в нормальном режиме идет экономия 16 байт за счет отсутствия ec в капче,
        // но ценой инстанцирования 2-х разных шаблонов
        if (ec)
            m_socket->get_io_service().post(
                  [boundHandler{
                      ExecOpType{ m_conn.get(), *m_socket, std::move(init.handler)
                    , std::forward<ResultCollector>(coll) }
                    }
                // Явное копирование ec в капче сделано намеренно. Если кто-то вдруг при рефакторинге кода объявит ec константой,
                // то это выражение спасает от ситуации, когда вся лямбда потеряет move-конструктор, что могло бы снизить производительность.
                , ec{ ec }] () mutable {
                    boundHandler(ec);
                    }
                );
        else
            m_socket->get_io_service().post(
                [boundHandler{
                      ExecOpType{ m_conn.get(), *m_socket, std::move(init.handler)
                    , std::forward<ResultCollector>(coll) }
                    }] () mutable {
                    boundHandler(boost::system::error_code{});
                    }
                );

        return init.result.get();
    }

    boost::system::error_code close() noexcept
    {
        boost::system::error_code ec;

        if (m_socket->is_open())
            m_socket->close(ec);

        m_conn.reset();

        return ec;
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
            if (-1 != (nativeSocket = ::PQsocket(m_conn.get())))
                if (!(ec = detail::dupTcpSocketFromHandle(nativeSocket, *m_socket)))
                    ec = m_socket->non_blocking(true, ec);
            else
                ec = make_error_code(PQError::CONN_INVALID_SOCKET);
        else
            ec = make_error_code(PQError::CONN_ALLOC_FAILED);

        using ConnOpType = detail::ConnectOp<decltype(init.handler)>;

        // здесь разделено на 2 разных вызова: в случае ошибки ec уходит в капчу,
        // а в нормальном режиме идет экономия 16 байт за счет отсутствия ec в капче,
        // но ценой инстанцирования 2-х разных шаблонов
        if (ec)
            m_socket->get_io_service().post(
                [boundHandler{ ConnOpType{ m_conn.get(), *m_socket, std::move(init.handler) } }, ec] () mutable {
                boundHandler(ec);
            });
        else
            m_socket->get_io_service().post(
                [boundHandler{ ConnOpType{ m_conn.get(), *m_socket, std::move(init.handler) } }]() mutable {
                boundHandler(boost::system::error_code{});
            });

        return init.result.get();
    }

private:
    std::unique_ptr<PGconn, decltype(&::PQfinish)> m_conn;
    std::unique_ptr<boost::asio::ip::tcp::socket> m_socket;
};

} // namespace asiopq
} // namespace ba
