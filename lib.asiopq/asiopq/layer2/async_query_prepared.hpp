#pragma once

#include "../layer1/connection.hpp"

namespace ba {
namespace asiopq {

template <typename Params, typename Handler, typename ResultCollector = IgnoreResult>
boost::system::error_code asyncQueryPrepared(Connection& conn, const char* name, const Params& params, bool textResultFormat, Handler&& handler, ResultCollector&& coll = {})
{
    conn.asyncExec(
        [pgConn{ conn.get() }, name, &params, textResultFormat]{
            if (!::PQsendQueryPrepared(pgConn, name, params.n(), params.values(), params.lengths(), params.formats(), textResultFormat ? 0 : 1))
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
