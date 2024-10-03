#pragma once

#include "connection_pool.hpp"
#include "../utility.hpp"

namespace ba {
namespace asiopq {

template <
      typename Operation
    , typename CompletionHandler
    , typename ConnectOp = PolymorphicOperationType
    >
class ReconnectionPool
    : public ConnectionPool<
          decltype(makeCheckedOperation(std::declval<Operation>(), std::declval<ConnectOp>()))
        , CompletionHandler
        >
{
    using Base = ConnectionPool<
          decltype(makeCheckedOperation(std::declval<Operation>(), std::declval<ConnectOp>()))
        , CompletionHandler
        >;

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
        : ReconnectionPool{
              ios
            , size
            , makeConnectOperation(std::move(params), expandDbname)
            }
    {
    }

    // потокобезопасен, как и в базовом классе
    template <typename OtherOp, typename OtherHandler>
    auto operator()(OtherOp&& op, OtherHandler&& handler)
    {
        return Base::operator()(
              makeCheckedOperation(std::forward<OtherOp>(op), m_connectOp)
            , std::forward<OtherHandler>(handler)
            );
    }

private:
    ConnectOp m_connectOp;
};

// make-функции позволяют вывести параметр шаблона ConnectOp
// пул не умеет ни копироваться, ни перемещаться, поэтому через unique_ptr

// Компилятор 3-й аргумент char* направляет в шаблонный ConnectOp&&, вместо std::string версию,
// поэтому проверяем connectOp на верную callable-сигнатуру через попытку конвертировать его в объект типа PolymorphicOperationType
template <
      typename Operation
    , typename CompletionHandler
    , typename ConnectOp
    , std::enable_if_t<std::is_convertible<ConnectOp, PolymorphicOperationType>::value, bool> = true
    >
auto makeReconnectionPool(boost::asio::io_service& ios, std::size_t size, ConnectOp&& connectOp)
{
    return std::make_unique<ReconnectionPool<
          Operation
        , CompletionHandler
        , std::decay_t<ConnectOp>
        >>(ios, size, std::forward<ConnectOp>(connectOp));
}

template <typename Operation, typename CompletionHandler>
auto makeReconnectionPool(boost::asio::io_service& ios, std::size_t size, std::string conninfo)
{
    return makeReconnectionPool<Operation, CompletionHandler>
        (ios, size, makeConnectOperation(std::move(conninfo)));
}

template <typename Operation, typename CompletionHandler>
auto makeReconnectionPool(
      boost::asio::io_service& ios
    , std::size_t size
    , std::map<std::string, std::string> params
    , bool expandDbname = false
    )
{
    return makeReconnectionPool<Operation, CompletionHandler>(
          ios
        , size
        , makeConnectOperation(std::move(params), expandDbname));
}

} // namespace asiopq
} // namespace ba
