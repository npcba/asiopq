#pragma once

#include "layer1/connection.hpp"

#include <map>
#include <vector>
#include <string>
#include <list>
#include <queue>
#include <memory>

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

#define BA_ASIOPQ_DECLARE_COMPOSE_OPERATOR_(operator_, pred)                   \
template<typename Op1, typename Op2>                                           \
auto operator operator_(Composed<Op1> op1, Op2&& op2)                          \
{                                                                              \
    return compose<pred>(std::move(op1.op), std::forward<Op2>(op2));           \
}                                                                              \
                                                                               \
template<typename Op1, typename Op2>                                           \
auto operator operator_(Op1&& op1, Composed<Op2> op2)                          \
{                                                                              \
    return compose<pred>(std::forward<Op1>(op1), std::move(op2.op));           \
}                                                                              \
                                                                               \
template<typename Op1, typename Op2>                                           \
auto operator operator_(Composed<Op1> op1, Composed<Op2> op2)                  \
{                                                                              \
    return compose<pred>(std::move(op1.op), std::move(op2.op));                \
}                                                                              \
/**/

// следующие операторы строят цепочки последовательных асинхронных операций (композитных) на Connection с грамматикой например:
// op1 | op2 & op3 + op4 ... (выполнить op1, если ошибочно, то выполнить op2, в случае успеха op3 и безусловно выполнить op4)
// группировка согласно приоритету операторов, либо нужно расставить скобки

BA_ASIOPQ_DECLARE_COMPOSE_OPERATOR_(+, Always)
BA_ASIOPQ_DECLARE_COMPOSE_OPERATOR_(|, IfError)
BA_ASIOPQ_DECLARE_COMPOSE_OPERATOR_(&, IfNotError)

#undef BA_LIBPQ_DECLARE_COMPOSE_OPERATOR_


using PolymorphicOperationType = std::function<void(Connection&, std::function<void(boost::system::error_code)>)>;


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

} // namespace asiopq
} // namespace ba
