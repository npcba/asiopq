#pragma once

#include "../layer1/connection.hpp"

namespace ba {
namespace asiopq {

template <typename Handler, typename ResultCollector = IgnoreResult>
auto asyncQuery(Connection& conn, const char* query, Handler&& handler, ResultCollector&& coll = {})
{
    return conn.asyncExec(
        [pgConn{ conn.get() }, query]{
            if (!::PQsendQuery(pgConn, query))
                return make_error_code(PQError::SEND_QUERY_FAILED);

            return boost::system::error_code{};
        },
        std::forward<Handler>(handler),
        std::forward<ResultCollector>(coll)
    );
}

} // namespace ba
} // namespace asiopq
