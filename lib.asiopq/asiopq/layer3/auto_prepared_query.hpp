#pragma once

#include <string>
#include <atomic>

#include <boost/asio/detail/bind_handler.hpp>
#include <boost/asio/detail/handler_invoke_helpers.hpp>
#include <boost/asio/async_result.hpp>

#include "../layer2/null_params.hpp"
#include "../layer2/async_prepare_params.hpp"
#include "../layer2/async_query_prepared.hpp"


namespace ba {
namespace asiopq {

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

    template <typename Params, typename Handler>
    void operator()(Params&& params, Handler&& handler)
    {
        if (m_prepared)
        {
            asyncQueryPrepared(m_conn, m_name.c_str(), std::forward<Params>(params), m_textResultFormat, std::forward<Handler>(handler));
            return;
        }

        boost::asio::detail::async_result_init<Handler, void(boost::system::error_code)> init{ std::forward<Handler>(handler) };
        auto hidden = [h{ std::move(init.handler) }](const boost::system::error_code& ec) mutable {
            boost::asio::detail::binder1<decltype(h), boost::system::error_code> binder{ std::move(h), ec };
            boost_asio_handler_invoke_helpers::invoke(binder, binder.handler_);
        };

        asyncPrepareParams(m_conn, m_name.c_str(), m_query.c_str(), m_prepareParams, [this, params{ std::forward<Params>(params) }, h{ std::move(hidden) }](const boost::system::error_code& ec) mutable {
            if (ec)
            {
            }

            m_prepared = true;
            asyncQueryPrepared(m_conn, m_name.c_str(), std::move(params), m_textResultFormat, std::move(h));
        });

        init.result.get();
    }

private:
    static std::string generateUniqueName()
    {
        static std::atomic_uint uniqueN;
        return std::to_string(++uniqueN);
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
