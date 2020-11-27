#pragma once

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/buffer.hpp>


namespace ba {
namespace asiopq {
namespace detail {

template <typename WaitHandler>
void asyncWaitReading(boost::asio::ip::tcp::socket& s, WaitHandler&& handler)
{
    s.async_read_some(
          boost::asio::null_buffers{}
        , [handler{ std::forward<WaitHandler>(handler) }]
          (const boost::system::error_code& ec, std::size_t) mutable { handler(ec); } // ignore size
        );
    // TS: s.async_wait(boost::asio::ip::tcp::socket::wait_read, ...);
}

template <typename WaitHandler>
void asyncWaitWriting(boost::asio::ip::tcp::socket& s, WaitHandler&& handler)
{
    s.async_write_some(
          boost::asio::null_buffers{}
        , [handler{ std::forward<WaitHandler>(handler) }]
          (const boost::system::error_code& ec, std::size_t) mutable { handler(ec); } // ignore size
        );
    // TS: s.async_wait(boost::asio::ip::tcp::socket::wait_write, ...);
}

} // namespace detail
} // namespace asiopq
} // namespace ba
