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

inline boost::asio::ip::tcp::socket dupTcpSocketFromHandle(boost::asio::io_service& ioService, boost::asio::ip::tcp::socket::native_handle_type handle)
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

} // namespace detail
} // namespace asiopq
} // namespace ba
