#pragma once

#include "async_prepare_params.hpp"
#include "params.hpp"

namespace ba {
namespace asiopq {

template <typename Handler, typename ResultCollector = IgnoreResult>
boost::system::error_code asyncPrepare(Connection& conn, const char* name, const char* query, Handler&& handler, ResultCollector&& coll = {})
{
    return asyncPrepareParams(conn, name, query, NullParams{}, std::forward<Handler>(handler), std::forward<ResultCollector>(coll));
}

} // namespace ba
} // namespace asiopq
