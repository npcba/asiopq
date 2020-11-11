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

class Connection
{
public:
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    explicit Connection(boost::asio::io_service& ioService)
        : m_socket { ioService }
    {
    }

    ~Connection() noexcept
    {
        close();
    }

    /*Connection(Connection&& other) noexcept(std::is_nothrow_move_constructible_v<decltype(m_socket)>)
        : m_socket{ std::move(other.m_socket) }
        , m_conn{ other.m_conn }
    {
        other.m_conn = nullptr;
    }

    Connection& operator=(Connection&& other) noexcept(std::is_nothrow_move_assignable_v<decltype(m_socket)>)
    {
        m_socket = std::move(other.m_socket);
        m_conn = other.m_conn;
        other.m_conn = nullptr;
    }*/

    PGconn* get() const noexcept
    {
        return m_conn;
    }

    template <typename ConnectHandler>
    void dispatchConnect(const boost::system::error_code& ec, ConnectHandler&& handler)
    {
        if (ec)
        {
            boost::asio::detail::binder1<std::remove_reference_t<ConnectHandler>, boost::system::error_code> h( handler, ec );
            boost_asio_handler_invoke_helpers::invoke(h, h.handler_);
            //handler(ec);
            return;
        }

        const auto pollResult = ::PQconnectPoll(m_conn);
        switch (pollResult)
        {
        case PGRES_POLLING_OK:
        {
            boost::asio::detail::binder1<std::remove_reference_t<ConnectHandler>, boost::system::error_code> h(handler, boost::system::error_code{});
            boost_asio_handler_invoke_helpers::invoke(h, h.handler_);
            // h(boost::system::error_code{});
            break;
        }
        case PGRES_POLLING_FAILED:
        {
            boost::asio::detail::binder1<std::remove_reference_t<ConnectHandler>, boost::system::error_code> h(handler, make_error_code(PQError::CONNECT_POLL_FAILED));
            boost_asio_handler_invoke_helpers::invoke(h, h.handler_);
            // h(make_error_code(PQError::CONNECT_POLL_FAILED));
            break;
        }
        case PGRES_POLLING_READING:
            m_socket.async_read_some(
                  boost::asio::null_buffers{}
                , [this, handler{ std::move(handler) }](const boost::system::error_code& ec, std::size_t) mutable {
                    dispatchConnect(ec, std::move(handler)); 
                    });
            break;
        case PGRES_POLLING_WRITING:
            m_socket.async_write_some(
                  boost::asio::null_buffers{}
                , [this, handler{ std::move(handler) }](const boost::system::error_code& ec, std::size_t) mutable {
                    dispatchConnect(ec, std::move(handler));
                    });
            break;
        default:
            break;
        }
    }

    template <typename ConnectHandler>
    void dispatchExec(const boost::system::error_code& ec, ConnectHandler&& handler)
    {
        if (ec)
        {
            handler(ec, nullptr);
            return;
        }

        /*const int flushed = ::PQflush(m_conn);
        if (1 == flushed)
        {
            m_socket.async_write_some(
                  boost::asio::null_buffers{}
                , [this, handler{ std::move(handler) }](const boost::system::error_code& ec, std::size_t) mutable {
                    dispatchExec(ec, std::move(handler));
                });

            return;
        }
        else if (0 != flushed)
        {

        }*/

        if (!::PQconsumeInput(m_conn))
        {
        }

        while (true)
        {
            if (::PQisBusy(m_conn))
            {
                m_socket.async_read_some(
                      boost::asio::null_buffers{}
                    , [this, handler{ std::move(handler) }](const boost::system::error_code& ec, std::size_t) mutable {
                        dispatchExec(ec, std::move(handler));
                    });

                return;
            }

            ::PGresult* res = ::PQgetResult(m_conn);
            handler(boost::system::error_code{}, res);

            if (!res)
                return;

            ::PQclear(res);
        }
    }

    template <typename ConnectHandler>
    void asyncConnect(const char* conninfo, ConnectHandler&& handler)
    {
        close();

        m_conn = ::PQconnectStart(conninfo);
        if (!m_conn)
            return;

        /*if (!::PQsetnonblocking(m_conn, 1))
        {
        }*/
        m_socket = dupTcpSocketFromHandle(m_socket.get_io_service(), ::PQsocket(m_conn));
        m_socket.non_blocking(true);

        using trueH = boost::asio::handler_type<ConnectHandler, void(boost::system::error_code)>::type;
        trueH h{ handler };
        boost::asio::async_result<trueH> res{ h };
        m_socket.get_io_service().post([this, h{ h }]() mutable {
            dispatchConnect(boost::system::error_code{}, h);
        });

        res.get();
    }

    template <typename Command, typename ExecHandler>
    void asyncExec(Command&& cmd, ExecHandler&& handler)
    {
        if (!cmd())
        {
        }

        //using trueH = boost::asio::handler_type<ExecHandler, void(boost::system::error_code)>::type;
        //trueH h{ handler };
        //boost::asio::async_result<trueH> res{ h };
        m_socket.get_io_service().post([this, handler{ std::move(handler) }]() mutable {
            dispatchExec(boost::system::error_code{}, std::move(handler));
        });

        //res.get();

    }

    void close() noexcept
    {
        if (m_socket.is_open())
        {
            boost::system::error_code ec;
            m_socket.close(ec);
        }

        if (m_conn)
        {
            ::PQfinish(m_conn);
            m_conn = nullptr;
        }
    }

private:
    boost::asio::ip::tcp::socket m_socket;
    PGconn* m_conn = nullptr;
};

} // asiopq
} // namespace ba
