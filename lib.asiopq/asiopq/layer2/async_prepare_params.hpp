#pragma once

#include "../layer1/connection.hpp"

namespace ba {
namespace asiopq {

template <typename Params, typename Handler, typename ResultCollector = IgnoreResult>
boost::system::error_code asyncPrepareParams(Connection& conn, const char* name, const char* query, const Params& params, Handler&& handler, ResultCollector&& coll = {})
{
    conn.asyncExec(
        [pgConn{ conn.get() }, name, query, &params]{
            if (!::PQsendPrepare(pgConn, name, query, params.n(), params.types()))
            {
            }
            return boost::system::error_code{};
        },
        std::forward<Handler>(handler),
        std::forward<ResultCollector>(coll)
    );

    return {};
}

} // namespace ba
} // namespace asiopq
