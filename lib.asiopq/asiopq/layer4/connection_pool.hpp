#pragma once

#include "../layer1/connection.hpp"

#include <list>
#include <queue>

#include <boost/asio/strand.hpp>


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
        if (0 == size)
            throw std::invalid_argument("ConnectionPool size can't be zero");

        while (size--)
            m_ready.emplace_back(ios);
    }

    // потокобезопасен, синхронизирован через strand
    template <typename OtherOp, typename OtherHandler>
    auto operator()(OtherOp&& op, OtherHandler&& handler)
    {
        boost::asio::detail::async_result_init<OtherHandler, void(boost::system::error_code, const Connection*)>
            init{ std::forward<OtherHandler>(handler) };

        m_strand.dispatch([this, op{ std::forward<OtherOp>(op) }, trueHandler{ std::move(init.handler) }]() mutable {
            if (m_ready.empty())
            {
                m_opQueue.emplace(std::move(op), std::move(trueHandler));
                return;
            }

            auto conn = m_ready.begin();
            setBusy(conn);

            m_strand.get_io_service().post([op{ std::move(op) }, this, conn, trueHandler{ std::move(trueHandler) }]() mutable {
                op(
                      *conn
                    , m_strand.wrap([this, conn, trueHandler{ std::move(trueHandler) }](const boost::system::error_code& ec) mutable {
                          handleExec(conn, std::move(trueHandler), ec);
                      })
                    );
            });
        });

        return init.result.get();
    }

private:
    template <typename Handler>
    void handleExec(
          std::list<Connection>::iterator conn
        , Handler&& handler
        , const boost::system::error_code& ec
        )
    {
        startOnePending(conn);

        boost::asio::detail::binder2<Handler, boost::system::error_code, const Connection*>
            binder{ std::forward<Handler>(handler), ec, &*conn };

        // TODO: вызвать handler вне strand, пользователь может подать тяжелый handler, пул будет заблокирован до его завершения
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

private:
    using TrueCompletionHandler =
        typename boost::asio::handler_type<
              CompletionHandler
            , void(boost::system::error_code, const Connection*)
            >::type;

private:
    boost::asio::io_service::strand m_strand;
    std::list<Connection> m_ready;
    std::list<Connection> m_busy;
    std::queue<std::pair<Operation, TrueCompletionHandler>> m_opQueue;
};

} // namespace asiopq
} // namespace ba
