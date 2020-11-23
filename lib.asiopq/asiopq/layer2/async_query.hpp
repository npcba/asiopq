#pragma once

#include "../layer1/connection.hpp"

namespace ba {
namespace asiopq {

template <typename Handler, typename ResultCollector = IgnoreResult>
boost::system::error_code asyncQuery(Connection& conn, const char* query, Handler&& handler, ResultCollector&& coll = {})
{
    conn.asyncExec(
        [pgConn{ conn.get() }, query] {
            if (!::PQsendQuery(pgConn, query))
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
