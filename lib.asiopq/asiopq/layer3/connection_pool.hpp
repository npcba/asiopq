#pragma once

#include <map>
#include <string>
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

    ConnectionPool(boost::asio::io_service& ios, std::size_t size, std::string conninfo)
        : m_strand{ ios }
        , m_conninfo{ std::move(conninfo) }
    {
        startPool(ios, size);
    }

    ConnectionPool(boost::asio::io_service& ios, std::size_t size, std::map<std::string, std::string> params, bool expandDbname = false)
        : m_strand{ ios }
        , m_params{ std::move(params) }
        , m_expandDbname{ expandDbname }
    {
        m_keywords.reserve(m_params.size() + 1);
        m_values.reserve(m_params.size() + 1);

        for (const auto& kv : m_params)
        {
            m_keywords.push_back(kv.first);
            m_values.push_back(kv.second);
        }

        m_keywords.push_back(nullptr);
        m_values.push_back(nullptr);

        startPool(ios, size);
    }

    template <typename OtherOp, typename OtherHandler>
    auto exec(OtherOp&& op, OtherHandler&& handler)
    {
        // If you get an error on the following line it means that your handler does
        // not meet the documented type requirements for a ConnectHandler:
        // void handler(
        //     const boost::system::error_code & ec // Result of operation
        // );
        boost::asio::ba_asiopq_handlerCheck(handler);

        boost::asio::detail::async_result_init<OtherHandler, void(boost::system::error_code)>
            init{ std::forward<OtherHandler>(handler) };

        m_strand.dispatch([this, op{ std::forward<OtherOp>(op) }, handler{ std::move(init.handler) }]() mutable {
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

        return init.result.get();
    }

private:
    void startPool(boost::asio::io_service& ios, std::size_t size)
    {
        if (0 == size)
            throw std::invalid_argument("ConnectionPool size can't be zero");

        while (size--)
            m_busy.emplace_back(ios);

        for (auto conn = m_busy.begin(); conn != m_busy.end(); ++conn)
            connect(conn);
    }

    void connect(std::list<Connection>::iterator conn)
    {
        if (m_keywords.empty())
            conn->asyncConnect(m_conninfo.c_str(), m_strand.wrap([this, conn](const boost::system::error_code& ec) { handleConnect(conn, ec); }));
        else
            conn->asyncConnectParams(m_keywords.data(), m_values.data(), int(m_expandDbname), m_strand.wrap([this, conn](const boost::system::error_code& ec) { handleConnect(conn, ec); }));
    }

    template <typename Handler>
    void handleExec(std::list<Connection>::iterator conn, Handler&& handler, const boost::system::error_code& ec)
    {
        startOnePending(conn);

        boost::asio::detail::binder1<Handler, boost::system::error_code> binder{ std::move(handler), ec };
        boost_asio_handler_invoke_helpers::invoke(binder, binder.handler_);
    }

    void handleConnect(std::list<Connection>::iterator conn, const boost::system::error_code& ec)
    {
        if (ec)
            return connect(conn);

        startOnePending(conn);
    }

    void setReady(std::list<Connection>::iterator conn)
    {
        m_ready.splice(m_ready.begin(), m_busy, conn);
    }

    void setBusy(std::list<Connection>::iterator conn)
    {
        m_busy.splice(m_busy.begin(), m_ready, conn);
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
    using TrueCompletionHandler = typename boost::asio::handler_type<CompletionHandler, void(boost::system::error_code)>::type;

private:
    boost::asio::io_service::strand m_strand;
    std::string m_conninfo;
    std::map<std::string, std::string> m_params;
    std::vector<const char*> m_keywords;
    std::vector<const char*> m_values;
    bool m_expandDbname = false;
    std::list<Connection> m_ready;
    std::list<Connection> m_busy;
    std::queue<std::pair<Operation, TrueCompletionHandler>> m_opQueue;
};

} // namespace asiopq
} // namespace ba
