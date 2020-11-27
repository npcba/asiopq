#pragma once

#include "../layer1/connection.hpp"

namespace ba {
namespace asiopq {

template <typename Params, typename Handler, typename ResultCollector = IgnoreResult>
auto asyncQueryParams(Connection& conn, const char* command, const Params& params, bool textResultFormat, Handler&& handler, ResultCollector&& coll = {})
{
    return conn.asyncExec(
        [pgConn{ conn.get() }, command, &params, textResultFormat]{
            if (!::PQsendQueryParams(pgConn, command, params.n(), params.types(), params.values(), params.lengths(), params.formats(), textResultFormat ? 0 : 1))
                return make_error_code(PQError::SEND_QUERY_PARAMS_FAILED);

            return boost::system::error_code{};
        },
        std::forward<Handler>(handler),
        std::forward<ResultCollector>(coll)
    );
}

} // namespace ba
} // namespace asiopq
