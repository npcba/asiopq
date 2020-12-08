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
            conn->asyncConnect("postgresql://ctest:ctest@localhost/ctest", [this, conn](const boost::system::error_code& ec) { handleConnect(conn, ec); });
    }

    template <typename Op, typename Handler>
    void exec(Op&& operation, Handler&& handler)
    {
        
        /*auto binder = [this, operation, handler](Connection& conn) {
            auto dispHandler = [this, handler, conn](const boost::system::error_code& ec) {dispatch(conn, handler); };
            operation(conn, dispHandler);
        };*/

        if (m_ready.empty())
        {
            m_opQueue.emplace(operation, handler);
            return;
        }

        auto conn = m_ready.begin();
        m_busy.splice(m_busy.end(), m_ready, conn);
        operation(*conn, [this, conn, handler](const boost::system::error_code& ec) {handleExec(conn, handler, ec); });
        
    }

private:
    using Queue = std::queue<std::pair<std::function<void(Connection&, std::function<void(const boost::system::error_code&)>)>, std::function<void(const boost::system::error_code&)>>>;
    /*void dispatch(std::list<Connection>::iterator conn, std::function<void(const boost::system::error_code&)> handler, const boost::system::error_code& ec)
    {
        startPending(conn);
        handler(ec);
    }p*/

    template <typename Handler>
    void handleExec(std::list<Connection>::iterator conn, Handler&& handler, const boost::system::error_code& ec)
    {
        startPending(conn);
        handler(ec);
    }

    void handleConnect(std::list<Connection>::iterator conn, const boost::system::error_code& ec)
    {
        if (ec)
            return conn->asyncConnect("postgresql://ctest:ctest@localhost/ctest", [this, conn](const boost::system::error_code& ec) { handleConnect(conn, ec); });

        startPending(conn);
    }

    void startPending(std::list<Connection>::iterator conn)
    {
        if (::PQstatus(conn->get()) != ::CONNECTION_OK)
            return conn->asyncConnect("postgresql://ctest:ctest@localhost/ctest", [this, conn](const boost::system::error_code& ec) { handleConnect(conn, ec); });

        if (m_opQueue.empty())
        {
            m_ready.splice(m_ready.end(), m_busy, conn);
        }
        else
        {
            m_opQueue.front().first(*conn, [this, conn, h{ m_opQueue.front().second }](const boost::system::error_code& ec) {handleExec(conn, h, ec); });
            m_opQueue.pop();
        }
    }

private:
    std::list<Connection> m_ready;
    std::list<Connection> m_busy;
    Queue m_opQueue;
};

} // namespace asiopq
} // namespace ba
