#pragma once

#include <list>
#include <queue>
#include <functional>

#include "../layer1/connection.hpp"

namespace ba {
namespace asiopq {

class ConnectionPool
{
public:
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;
    ConnectionPool(ConnectionPool&&) = delete;
    ConnectionPool& operator=(ConnectionPool&&) = delete;

    ConnectionPool(boost::asio::io_service& ios, std::size_t size)
    {
        while (size--)
            m_busy.emplace_back(ios);

        for (auto conn = m_busy.begin(); conn != m_busy.end(); ++conn)
            conn->asyncConnect("postgresql://ctest:ctest@localhost/ctest", [this, conn](const boost::system::error_code& ec) { dispatch(conn, [](const boost::system::error_code& ec) {}); });
    }

    template<typename Op, typename Handler>
    void exec(Op&& operation, Handler&& handler)
    {
        m_opQueue.emplace(std::forward<Op>(operation));
        if (m_ready.empty())
            return;

        auto conn = m_ready.begin();
        m_busy.splice(m_busy.end(), m_ready, conn);
        dispatch(conn, handler);
    }

private:
    template <typename Handler>
    void dispatch(std::list<Connection>::iterator conn, Handler&& handler)
    {
        if (::PQstatus(conn->get()) != ::CONNECTION_OK)
            return conn->asyncConnect("postgresql://ctest:ctest@localhost/ctest", [this, conn, handler](const boost::system::error_code& ec) { dispatch(conn, handler); });
        
        handler(boost::system::error_code{});

        if (m_opQueue.empty())
        {
            m_ready.splice(m_ready.end(), m_busy, conn);
            return;
        }

        m_opQueue.front()(*conn, [this, conn, handler](const boost::system::error_code& ec) {dispatch(conn, handler); });
        m_opQueue.pop();
    }

private:
    std::list<Connection> m_ready;
    std::list<Connection> m_busy;
    std::queue<std::function<void(Connection&, std::function<void(const boost::system::error_code&)>)>> m_opQueue;
};

} // namespace asiopq
} // namespace ba
