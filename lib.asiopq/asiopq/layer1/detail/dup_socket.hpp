#pragma once

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

#ifdef _WIN32
#   include <WinSock2.h>
#else
#   include <unistd.h>
#   include <sys/socket.h>
#endif

namespace ba {
namespace asiopq {
namespace detail {

inline boost::system::error_code
dupTcpSocketFromHandle(
      boost::asio::ip::tcp::socket::native_handle_type handle
    , boost::asio::ip::tcp::socket& sock
    )
{
#ifdef _WIN32
    ::WSAPROTOCOL_INFOW protoInfo;
    if (0 != ::WSADuplicateSocketW(handle, ::GetCurrentProcessId(), &protoInfo))
        return boost::system::error_code{
            ::WSAGetLastError()
            , boost::asio::error::get_system_category()
            };

    const auto dupHandle = ::WSASocketW(
          protoInfo.iAddressFamily
        , protoInfo.iSocketType
        , protoInfo.iProtocol
        , &protoInfo
        , 0
        , WSA_FLAG_OVERLAPPED
        );

    if (INVALID_SOCKET == dupHandle)
        return boost::system::error_code{
            ::WSAGetLastError()
            , boost::asio::error::get_system_category()
        };

    const auto family = protoInfo.iAddressFamily;

#else // POSIX
    const auto dupHandle = ::dup(handle);
    if (dupHandle < 0)
        return boost::system::error_code{
              errno
            , boost::asio::error::get_system_category()
            };

    sockaddr_storage name;
    socklen_t nameLen = sizeof(name);
    const auto err = ::getsockname(dupHandle, reinterpret_cast<sockaddr*>(&name), &nameLen);
    if (0 != err)
        return boost::system::error_code{
              errno
            , boost::asio::error::get_system_category()
            };

    const auto family = name.ss_family;
#endif

    boost::system::error_code ec;

    // должны были передать закрытый сокет, но на всякий случай проверим и закроем
    if (sock.is_open())
    {
        assert("socket is open");
        sock.close(ec);  // игнорируем ошибку закрытия, если что, вывалится в assign
    }

    sock.assign(
          AF_INET6 == family ? boost::asio::ip::tcp::v6() : boost::asio::ip::tcp::v4()
        , dupHandle
        , ec
        );

    return ec;
}

} // namespace detail
} // namespace asiopq
} // namespace ba
