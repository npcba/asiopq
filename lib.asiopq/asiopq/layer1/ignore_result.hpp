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

        switch (::PQresultStatus(res))
        {
        case PGRES_BAD_RESPONSE:
            return make_error_code(PQError::RESULT_BAD_RESPONSE);
        case PGRES_FATAL_ERROR:
            return make_error_code(PQError::RESULT_FATAL_ERROR);
        default:
            return {}; // нет ошибки
        }
    }
};

} // namespace asiopq
} // namespace ba
