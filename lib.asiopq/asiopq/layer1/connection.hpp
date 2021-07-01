#pragma once

#include <memory>

#include <libpq-fe.h>

#include <boost/asio/async_result.hpp>
#include <boost/lexical_cast.hpp>

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

namespace detail {

// TODO: этот шаблон я стащил из boost 1.62, в новом boost его нет. Нужно разобрться, что вместо него в новом пользуют.
template <typename Handler, typename Signature>
struct async_result_init
{
  explicit async_result_init(BOOST_ASIO_MOVE_ARG(Handler) orig_handler)
    : handler(BOOST_ASIO_MOVE_CAST(Handler)(orig_handler)),
      result(handler)
  {
  }

  typename boost::asio::handler_type<Handler, Signature>::type handler;
  boost::asio::async_result<typename boost::asio::handler_type<Handler, Signature>::type> result;
};

} // namespace detail

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
        assert(m_conn);

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
        assert(m_conn);

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

        detail::async_result_init<ExecHandler, void(boost::system::error_code)>
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
        detail::async_result_init<ConnectHandler, void(boost::system::error_code)>
            init{ std::forward<ConnectHandler>(handler) };

        boost::system::error_code ec;
        int nativeSocket = -1;

        if (m_conn)
        {
            if (::PQstatus(m_conn.get()) == ::CONNECTION_BAD)
            {
                ec = make_error_code(PQError::CONN_FAILED);
            }
            else if (-1 != (nativeSocket = ::PQsocket(m_conn.get())))
            {
                if (!(ec = detail::dupTcpSocketFromHandle(nativeSocket, *m_socket)))
                {
                    ec = m_socket->non_blocking(true, ec);
                }
            }
            else
            {
                ec = make_error_code(PQError::CONN_INVALID_SOCKET);
            }
        }
        else
        {
            ec = make_error_code(PQError::CONN_ALLOC_FAILED);
        }

        boost::posix_time::time_duration::sec_type timeout = 0;
        if (!ec)
            timeout = parseConnectTimeout(ec);

        using ConnOpType = detail::ConnectOp<decltype(init.handler)>;

        // здесь разделено на 2 разных вызова: в случае ошибки ec уходит в капчу,
        // а в нормальном режиме идет экономия 16 байт за счет отсутствия ec в капче,
        // но ценой инстанцирования 2-х разных шаблонов
        if (ec)
            m_socket->get_io_service().post(
                [boundHandler{ ConnOpType{ m_conn.get(), *m_socket, timeout, std::move(init.handler) } }, ec] () mutable {
                boundHandler(ec);
            });
        else
            m_socket->get_io_service().post(
                [boundHandler{ ConnOpType{ m_conn.get(), *m_socket, timeout, std::move(init.handler) } }]() mutable {
                boundHandler(boost::system::error_code{});
            });

        return init.result.get();
    }

    /// Выделяет из параметров подключения connect_timeout.
    /// Поведение соответствует обработке этого параметра самой libpq, когда она работает в синхронном режиме.
    /// В асинхронном режиме обработка connect_timeout возложена на пользователя libpq, чем мы тут и занимаемся.
    boost::posix_time::time_duration::sec_type parseConnectTimeout(boost::system::error_code& ec)
    {
        ec = {};
        boost::posix_time::time_duration::sec_type connTimeout = 0;

        ::PQconninfoOption* const opts = ::PQconninfo(m_conn.get());
        for (const auto* curOpt = opts; curOpt->keyword; ++curOpt)
        {
            if (std::strcmp(curOpt->keyword, "connect_timeout") != 0)
                continue;

            // мы тут, потому что это connect_timeout

            if (nullptr == curOpt->val) // нет значения, значит отставляем 0
                break;

            // есть значение, парсим его как целочисленное
            
            try
            {
                connTimeout = boost::lexical_cast<decltype(connTimeout)>(curOpt->val);
                if (connTimeout < 0)
                    connTimeout = 0; // также действует libpq, <=0, или отсутствующий параметр - значит без таймаута
                else if(connTimeout < 2)
                    connTimeout = 2; // также действует libpq, меньше 2-х нельзя
            }
            catch (const std::exception&)
            {
                assert(!u8"Сюда не должны приходить. Если libpq распарсил число, то и мы должны");
                ec = make_error_code(PQError::CONN_FAILED); // Но на всякий случай обрабатываем, мало ли, как libpq парсит
            }

            break; // нашли значение, дальше не идем
        }

        ::PQconninfoFree(opts);
        return connTimeout;
    }

private:
    std::unique_ptr<PGconn, decltype(&::PQfinish)> m_conn;
    std::unique_ptr<boost::asio::ip::tcp::socket> m_socket;
};

} // namespace asiopq
} // namespace ba
