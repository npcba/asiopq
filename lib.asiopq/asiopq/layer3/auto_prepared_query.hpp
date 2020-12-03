#pragma once

#include <string>
#include <atomic>

#include <boost/asio/detail/bind_handler.hpp>
#include <boost/asio/detail/handler_invoke_helpers.hpp>
#include <boost/asio/async_result.hpp>

#include "../layer2/async_prepare_params.hpp"
#include "../layer2/async_query_prepared.hpp"
#include "cloned_params.hpp"


namespace ba {
namespace asiopq {

// функтор, который автоматически выполняет asyncPrepareParams один раз перед первым вызовом asyncQueryPrepared,
// сам генерит уникальное имя команды и хранит его в своем инстансе
template <typename PrepareParams = NullParams>
class AutoPreparedQuery
{
    AutoPreparedQuery(const AutoPreparedQuery&) = delete;
    AutoPreparedQuery& operator=(const AutoPreparedQuery&) = delete;

public:
    AutoPreparedQuery(Connection& conn, std::string query, bool textResultFormat = true, PrepareParams prepareParams = {})
        : m_conn{ conn }
        , m_query{ std::move(query) }
        , m_prepareParams{ std::move(prepareParams) }
        , m_name{ generateUniqueName() }
        , m_textResultFormat{ textResultFormat }
    {
    }

    template <typename Params, typename CompletionToken, typename ResultCollector = IgnoreResult>
    auto operator()(Params&& params, CompletionToken&& completion, ResultCollector&& coll = {})
    {
        if (m_prepared)
            return asyncQueryPrepared(
                  m_conn
                , m_name.c_str()
                , std::forward<Params>(params)
                , m_textResultFormat
                , std::forward<CompletionToken>(completion)
                , std::forward<ResultCollector>(coll)
                );

        boost::asio::detail::async_result_init<CompletionToken, void(boost::system::error_code)>
            init{ std::forward<CompletionToken>(completion) };

        // нужно скрыть реальный тип init.handler,
        // чтобы хук boost::asio::async_result не распознал его и не вызвал лишний раз .get,
        // потому что init.result.get() будет вызван в текущей функции
        auto hiddenHandler = [handler{ std::move(init.handler) }](const boost::system::error_code& ec) mutable {
            boost::asio::detail::binder1<decltype(handler), boost::system::error_code>
                binder{ std::move(handler), ec };
            boost_asio_handler_invoke_helpers::invoke(binder, binder.handler_);
        };

        asyncPrepareParams(
              m_conn
            , m_name.c_str()
            , m_query.c_str()
            , m_prepareParams
            , [
                  this
                , params{ passOrClone(std::forward<Params>(params)) }
                , hiddenHandler{ std::move(hiddenHandler) }
                , coll{ std::forward<ResultCollector>(coll) }
              ](const boost::system::error_code& ec) mutable {
                if (ec)
                    return hiddenHandler(ec);

                m_prepared = true;
                asyncQueryPrepared(
                      m_conn
                    , m_name.c_str()
                    , std::move(params)
                    , m_textResultFormat
                    , std::move(hiddenHandler)
                    , std::move(coll)
                    );
              }
            );

        return init.result.get();
    }

private:
    static std::string generateUniqueName()
    {
        static std::atomic_uint uniqueN;
        return std::to_string(++uniqueN);
    }

    template <typename Params, typename = std::enable_if_t<ParamsTraits<std::decay_t<Params>>::IsOwner::value>>
    static Params&& passOrClone(Params&& params) // pass
    {
        return std::forward<Params>(params);
    }

    template <typename Params, typename = std::enable_if_t<!ParamsTraits<std::decay_t<Params>>::IsOwner::value>>
    static ClonedParams passOrClone(Params&& params) // clone
    {
        return { std::forward<Params>(params) };
    }

private:
    Connection& m_conn;
    const std::string m_query;
    const PrepareParams m_prepareParams;
    const std::string m_name;
    const bool m_textResultFormat = true;
    bool m_prepared = false;
};

} // namespace ba
} // namespace asiopq
