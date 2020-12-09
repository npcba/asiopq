#pragma once

#include <list>
#include <queue>

#include "../layer1/connection.hpp"

namespace ba {
namespace asiopq {

template <typename Operation, typename CompletionHandler>
class ConnectionPool
{
public:
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;
    ConnectionPool(ConnectionPool&&) = delete;
    ConnectionPool& operator=(ConnectionPool&&) = delete;

    ConnectionPool(boost::asio::io_service& ios, std::size_t size)
        : m_strand{ ios }
    {
        while (size--)
            m_busy.emplace_back(ios);

        for (auto conn = m_busy.begin(); conn != m_busy.end(); ++conn)
            connect(conn);
    }

    template <typename OtherOp, typename OtherHandler>
    void exec(OtherOp&& op, OtherHandler&& handler)
    {
        m_strand.dispatch([this, op{ std::forward<OtherOp>(op) }, handler{ std::forward<OtherHandler>(handler) }]() mutable {
            if (m_ready.empty())
            {
                m_opQueue.emplace(std::move(op), std::move(handler));
                return;
            }

            auto conn = m_ready.begin();
            setBusy(conn);
            m_strand.get_io_service().post([op{ std::move(op) }, this, conn, handler{ std::move(handler) }]() mutable {
                op(*conn, m_strand.wrap([this, conn, handler{ std::move(handler) }](const boost::system::error_code& ec) mutable { handleExec(conn, std::move(handler), ec); }));
            });
        });
    }

private:
    void connect(std::list<Connection>::iterator conn)
    {
        conn->asyncConnect("postgresql://ctest:ctest@localhost/ctest", m_strand.wrap([this, conn](const boost::system::error_code& ec) { handleConnect(conn, ec); }));
    }

    template <typename Handler>
    void handleExec(std::list<Connection>::iterator conn, Handler&& handler, const boost::system::error_code& ec)
    {
        startOnePending(conn);
        std::forward<Handler>(handler)(ec);
    }

    void handleConnect(std::list<Connection>::iterator conn, const boost::system::error_code& ec)
    {
        if (ec)
            return connect(conn);

        startOnePending(conn);
    }

    void setReady(std::list<Connection>::iterator conn)
    {
        m_ready.splice(m_ready.end(), m_busy, conn);
    }

    void setBusy(std::list<Connection>::iterator conn)
    {
        m_busy.splice(m_busy.end(), m_ready, conn);
    }
    
    void startOnePending(std::list<Connection>::iterator conn)
    {
        if (::PQstatus(conn->get()) != ::CONNECTION_OK)
            return connect(conn);

        if (m_opQueue.empty())
        {
            setReady(conn);
        }
        else
        {
            auto& pair = m_opQueue.front();
            m_strand.get_io_service().post([pair{ std::move(pair) }, this, conn]() mutable {
                auto& op = pair.first;
                auto& handler = pair.second;
                op(*conn, m_strand.wrap([this, conn, handler{ std::move(handler) }](const boost::system::error_code& ec) mutable { handleExec(conn, std::move(handler), ec); }));
            });
            m_opQueue.pop();
        }
    }

private:
    std::list<Connection> m_ready;
    std::list<Connection> m_busy;
    std::queue<std::pair<Operation, CompletionHandler>> m_opQueue;
    boost::asio::io_service::strand m_strand;
};

} // namespace asiopq
} // namespace ba
