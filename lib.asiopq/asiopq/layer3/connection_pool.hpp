#pragma once

#include <map>
#include <vector>
#include <string>
#include <list>
#include <queue>

#include "../layer1/connection.hpp"

namespace ba {
namespace asiopq {

template <
      typename Operation
    , typename CompletionHandler
    , typename ConnectCmd = std::function<void (Connection&, std::function<void(boost::system::error_code)>)>
    >
class ConnectionPool
{
public:
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;
    ConnectionPool(ConnectionPool&&) = delete;
    ConnectionPool& operator=(ConnectionPool&&) = delete;

    ConnectionPool(boost::asio::io_service& ios, std::size_t size, ConnectCmd connectCmd)
        : m_strand{ ios }
        , m_connectCmd{ std::move(connectCmd) }
    {
        startPool(ios, size);
    }

    ConnectionPool(boost::asio::io_service& ios, std::size_t size, std::string conninfo)
        : ConnectionPool{ ios, size,  makeConnectCmd(std::move(conninfo))}
    {
    }

    ConnectionPool(
          boost::asio::io_service& ios
        , std::size_t size
        , std::map<std::string, std::string> params
        , bool expandDbname = false
        )
        : ConnectionPool{ ios, size,  makeConnectCmd(std::move(params), expandDbname) }
    {
    }

    template <typename OtherOp, typename OtherHandler>
    auto exec(OtherOp&& op, OtherHandler&& handler)
    {
        boost::asio::detail::async_result_init<OtherHandler, void(boost::system::error_code, const Connection*)>
            init{ std::forward<OtherHandler>(handler) };

        m_strand.dispatch([this, op{ std::forward<OtherOp>(op) }, handler{ std::move(init.handler) }]() mutable {
            if (m_ready.empty())
            {
                m_opQueue.emplace(std::move(op), std::move(handler));
                return;
            }

            auto conn = m_ready.begin();
            setBusy(conn);

            if (::PQstatus(conn->get()) != ::CONNECTION_OK)
            {
                // сохраним операцию в очередь и запустим переподключение соединения
                m_opQueue.emplace(std::move(op), std::move(handler));
                connect(conn);
                return;
            }

            m_strand.get_io_service().post([op{ std::move(op) }, this, conn, handler{ std::move(handler) }]() mutable {
                op(
                      *conn
                    , m_strand.wrap([this, conn, handler{ std::move(handler) }](const boost::system::error_code& ec) mutable {
                          handleExec(conn, std::move(handler), ec);
                      })
                    );
            });
        });

        return init.result.get();
    }

private:
    auto makeConnectCmd(std::string&& conninfo)
    {
        return [conninfo{ std::move(conninfo) }](Connection& conn, auto&& handler){
            conn.asyncConnect(conninfo.c_str(), std::forward<decltype(handler)>(handler));
        };
    }

    auto makeConnectCmd(std::map<std::string, std::string>&& params, bool expandDbname)
    {
        std::vector<const char*> keywords;
        std::vector<const char*> values;

        keywords.reserve(params.size() + 1);
        values.reserve(params.size() + 1);

        for (const auto& kv : params)
        {
            keywords.push_back(kv.first.c_str());
            values.push_back(kv.second.c_str());
        }

        keywords.push_back(nullptr);
        values.push_back(nullptr);

        return [
              params{ std::move(params) }
            , keywords{ std::move(keywords) }
            , values{ std::move(values) }
            , expandDbname
            ](Connection& conn, auto&& handler)
        {
            conn.asyncConnectParams(
                  keywords.data()
                , values.data()
                , int(expandDbname)
                , std::forward<decltype(handler)>(handler)
                );
        };
    }

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
        m_connectCmd(*conn, m_strand.wrap([this, conn](const boost::system::error_code& ec) {
            handleConnect(conn, ec);
        }));
    }

    template <typename Handler>
    void handleExec(
          std::list<Connection>::iterator conn
        , Handler&& handler
        , const boost::system::error_code& ec
        )
    {
        if (::PQstatus(conn->get()) != CONNECTION_OK)
            connect(conn);
        else
            startOnePending(conn);

        invokeHandler(std::forward<Handler>(handler), ec, &*conn);
    }

    void handleConnect(std::list<Connection>::iterator conn, const boost::system::error_code& ec)
    {
        // если подключение неудачно, то обрадуем об этом первую из ожидающих операций в очереди
        if (ec)
            errorOnePending(conn, ec);
        else
            startOnePending(conn);
    }

    template <typename Handler>
    void invokeHandler(Handler&& handler, const boost::system::error_code& ec, const Connection* conn)
    {
        boost::asio::detail::binder2<Handler, boost::system::error_code, const Connection*>
            binder{ std::forward<Handler>(handler), ec, conn };

        boost_asio_handler_invoke_helpers::invoke(binder, binder.handler_);
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
        if (m_opQueue.empty())
        {
            // объявляем conn свободным, т.к. очередь операций пуста
            setReady(conn);
            return;
        }

        auto& pair = m_opQueue.front();

        m_strand.get_io_service().post([pair{ std::move(pair) }, this, conn]() mutable {
            auto& op = pair.first;
            auto& handler = pair.second;
            op(*conn, m_strand.wrap([this, conn, handler{ std::move(handler) }](const boost::system::error_code& ec) mutable {
                handleExec(conn, std::move(handler), ec);
            }));
        });

        m_opQueue.pop();
    }

    void errorOnePending(std::list<Connection>::iterator conn, const boost::system::error_code& ec)
    {
        if (m_opQueue.empty())
        {
            setReady(conn);
            return;
        }
        
        invokeHandler(std::move(m_opQueue.front().second), ec, &*conn);
        m_opQueue.pop();
    }

private:
    using TrueCompletionHandler =
        typename boost::asio::handler_type<
              CompletionHandler
            , void(boost::system::error_code, const Connection*)
            >::type;

private:
    boost::asio::io_service::strand m_strand;
    ConnectCmd m_connectCmd;;
    std::list<Connection> m_ready;
    std::list<Connection> m_busy;
    std::queue<std::pair<Operation, TrueCompletionHandler>> m_opQueue;
};

} // namespace asiopq
} // namespace ba
