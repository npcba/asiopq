#pragma once

#include <functional>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/buffer.hpp>

namespace ba {
namespace asiopq {
namespace detail {

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

} // namespace detail
} // namespace asiopq
} // namespace ba
