#pragma once

#include <libpq-fe.h>

#include <boost/system/error_code.hpp>


namespace ba {
namespace asiopq {

struct IgnoreResult
{
    boost::system::error_code operator()(const ::PGresult* res) const noexcept
    {
        if (!res) // конец данных
            return {};

        const auto status = ::PQresultStatus(res);
        if (PGRES_FATAL_ERROR == status)
            return make_error_code(PQError::RESULT_FATAL_ERROR);
        if (PGRES_BAD_RESPONSE == status)
            return make_error_code(PQError::RESULT_BAD_RESPONSE);

        return {};
    }
};

} // namespace asiopq
} // namespace ba
