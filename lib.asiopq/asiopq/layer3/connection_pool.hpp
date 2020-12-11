#pragma once

#include <map>
#include <vector>
#include <string>
#include <list>
#include <queue>
#include <memory>

#include "../layer1/connection.hpp"

namespace ba {
namespace asiopq {

template<typename Op>
struct Composed
{
    template <typename Handler>
    void operator()(Connection& conn, Handler&& handler)
    {
        op(conn, std::forward<Handler>(handler));
    }

    template <typename Handler>
    void operator()(Connection& conn, Handler&& handler) const
    {
        op(conn, std::forward<Handler>(handler));
    }

    Op op;
};

template <typename Op>
Composed<std::decay_t<Op>> compose(Op&& op)
{
    return { std::forward<Op>(op) };
}

// чтобы избежать рекурсивного вкладывания
template <typename Op>
Composed<Op> compose(Composed<Op> op)
{
    return op;
}

// TODO: добавить перегрузку для предиката с состоянием (нам пока не нужен)
template<typename Pred, typename Op1, typename Op2>
auto compose(Op1&& op1, Op2&& op2)
{
    return compose([op1{ std::forward<Op1>(op1) }, op2{ std::forward<Op2>(op2) }](Connection& conn, auto&& handler) mutable {
        op1(conn, [op2{ std::move(op2) }, &conn, handler{ std::move(handler) }](const boost::system::error_code& ec) mutable {
            if (Pred{}(ec, conn))
            {
                op2(conn, std::move(handler));
            }
            else
            {
                boost::asio::detail::binder1<decltype(handler), boost::system::error_code> binder{ std::move(handler), ec };
                boost_asio_handler_invoke_helpers::invoke(binder, binder.handler_);
            }
        });
    });
}

struct Always { bool operator()(const boost::system::error_code&, const Connection&) const { return true; } };
struct IfError { bool operator()(const boost::system::error_code& ec, const Connection&) const { return bool(ec); } };
struct IfNotError{ bool operator()(const boost::system::error_code& ec, const Connection&) const { return !ec; } };

#define BA_ASIOPQ_DECLARE_COMPOSE_OPERATOR_(operator_, pred) \
template<typename Op1, typename Op2> \
auto operator operator_(Composed<Op1> op1, Op2&& op2) \
{ \
    return compose<pred>(std::move(op1.op), std::forward<Op2>(op2)); \
} \
\
template<typename Op1, typename Op2> \
auto operator operator_(Op1&& op1, Composed<Op2> op2) \
{ \
    return compose<pred>(std::forward<Op1>(op1), std::move(op2.op)); \
} \
\
template<typename Op1, typename Op2> \
auto operator operator_(Composed<Op1> op1, Composed<Op2> op2) \
{ \
    return compose<pred>(std::move(op1.op), std::move(op2.op)); \
} \
/**/

BA_ASIOPQ_DECLARE_COMPOSE_OPERATOR_(+, Always)
BA_ASIOPQ_DECLARE_COMPOSE_OPERATOR_(|, IfError)
BA_ASIOPQ_DECLARE_COMPOSE_OPERATOR_(&, IfNotError)

#undef BA_LIBPQ_DECLARE_COMPOSE_OPERATOR_

using PolymorphicOperationType = std::function<void(Connection&, std::function<void(boost::system::error_code)>)>;

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
    auto exec(OtherOp&& op, OtherHandler&& handler)
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

template <typename Op, typename ConnectOp>
auto makeCheckedOperation(Op op, ConnectOp connectOp)
{
    struct IfDisconnected
    {
        bool operator()(const boost::system::error_code& ec, const Connection& conn) const
        {
            return bool(ec) && ::PQstatus(conn.get()) != ::CONNECTION_OK;
        }
    };

    // нужно 2 копии операции: для первого вызова и повторного, в случае неудачи
    // TODO: намудрить со ссылками или указателями или проксями, чтобы ссылаться на один инстанс
    auto copy{ compose(op) };
    return compose<IfDisconnected>(std::move(op), std::move(connectOp) & std::move(copy));
}

inline auto makeConnectOperation(std::string&& conninfo)
{
    return [conninfo{ std::move(conninfo) }](Connection& conn, auto&& handler){
        conn.asyncConnect(conninfo.c_str(), std::forward<decltype(handler)>(handler));
    };
}

inline auto makeConnectOperation(std::map<std::string, std::string>&& params, bool expandDbname = false)
{
    struct Capture
    {
        std::map<std::string, std::string> params;
        std::vector<const char*> keywordsView;
        std::vector<const char*> valuesView;
        bool expandDbname;
    };

    // тот самый случай, когда shared_ptr полезен, параметры жирные, но иммутабельные, пусть все копии лямбды шарят их,
    // иначе лямбда станет слишком жирной
    auto capture = std::make_shared<Capture>();
    auto& capturedParams = capture->params = std::move(params);
    capture->expandDbname = expandDbname;

    auto& keywordsView = capture->keywordsView;;
    auto& valuesView = capture->valuesView;
    keywordsView.reserve(capturedParams.size() + 1);
    valuesView.reserve(capturedParams.size() + 1);

    for (const auto& kv : capturedParams)
    {
        keywordsView.push_back(kv.first.c_str());
        valuesView.push_back(kv.second.c_str());
    }

    keywordsView.push_back(nullptr);
    valuesView.push_back(nullptr);

    return [capture{ std::move(capture) }](Connection& conn, auto&& handler)
    {
        conn.asyncConnectParams(
                capture->keywordsView.data()
            , capture->valuesView.data()
            , int(capture->expandDbname)
            , std::forward<decltype(handler)>(handler)
            );
    };
}

template <
      typename Operation
    , typename CompletionHandler
    , typename ConnectOp = PolymorphicOperationType
    >
class ReconnectionPool
    : public ConnectionPool<decltype(makeCheckedOperation(std::declval<Operation>(), std::declval<ConnectOp>())), CompletionHandler>
{
    using Base = ConnectionPool<decltype(makeCheckedOperation(std::declval<Operation>(), std::declval<ConnectOp>())), CompletionHandler>;

public:
    ReconnectionPool(const ReconnectionPool&) = delete;
    ReconnectionPool& operator=(const ReconnectionPool&) = delete;
    ReconnectionPool(ReconnectionPool&&) = delete;
    ReconnectionPool& operator=(ReconnectionPool&&) = delete;

    ReconnectionPool(boost::asio::io_service& ios, std::size_t size, ConnectOp&& connectOp)
        : Base{ ios, size }
        , m_connectOp{ std::move(connectOp) }
    {
    }

    ReconnectionPool(boost::asio::io_service& ios, std::size_t size, const ConnectOp& connectOp)
        : Base{ ios, size }
        , m_connectOp{ connectOp }
    {
    }

    ReconnectionPool(boost::asio::io_service& ios, std::size_t size, std::string conninfo)
        : ReconnectionPool{ ios, size,  makeConnectOperation(std::move(conninfo))}
    {
    }

    ReconnectionPool(
          boost::asio::io_service& ios
        , std::size_t size
        , std::map<std::string, std::string> params
        , bool expandDbname = false
        )
        : ReconnectionPool{ ios, size,  makeConnectOperation(std::move(params), expandDbname) }
    {
    }

    // потокобезопасен, как и базовый класс
    template <typename OtherOp, typename OtherHandler>
    auto exec(OtherOp&& op, OtherHandler&& handler)
    {
        return Base::exec(makeCheckedOperation(std::forward<OtherOp>(op), m_connectOp), std::forward<OtherHandler>(handler));
    }

private:
    ConnectOp m_connectOp;
};

// make-функции позволяют вывести параметр шаблона ConnectOp
// пул не умеет ни копироваться, ни перемещаться, поэтому через unique_ptr

struct SFINAECheck_
{
    static auto operationSignature()
    {
        return std::declval<PolymorphicOperationType>();
    }
};

// тут нужен SFINAE, т.к. компилятор 3-й аргумент char* направляет в шаблонный  ConnectOp&&, вместо std::string версию,
// поэтому проверяем connectOp на верную callable-сигнатуру через попытку присвоить его в объект типа PolymorphicOperationType
template <typename Operation, typename CompletionHandler, typename ConnectOp, typename = decltype(SFINAECheck_::operationSignature() = std::declval<ConnectOp>())>
auto makeReconnectionPool(boost::asio::io_service& ios, std::size_t size, ConnectOp&& connectOp)
{
    return std::make_unique<ReconnectionPool<Operation, CompletionHandler, std::decay_t<ConnectOp>>>(ios, size, std::forward<ConnectOp>(connectOp));
}

template <typename Operation, typename CompletionHandler>
auto makeReconnectionPool(boost::asio::io_service& ios, std::size_t size, std::string conninfo)
{
    return makeReconnectionPool<Operation, CompletionHandler>(ios, size, makeConnectOperation(std::move(conninfo)));
}

template <typename Operation, typename CompletionHandler>
auto makeReconnectionPool(boost::asio::io_service& ios, std::size_t size, std::map<std::string, std::string> params, bool expandDbname = false)
{
    return makeReconnectionPool<Operation, CompletionHandler>(ios, size, makeConnectOperation(std::move(params), expandDbname));
}

} // namespace asiopq
} // namespace ba
