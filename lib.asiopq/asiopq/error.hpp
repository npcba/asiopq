#pragma once

#include <type_traits>
#include <boost/system/error_code.hpp>
#include <libpq-fe.h>

namespace ba {
namespace asiopq {

enum class PQError : int
{
    OK = 0,
    CONN_ALLOC_FAILED,
    CONN_INVALID_SOCKET,
    CONN_POLL_FAILED,
    CONSUME_INPUT_FAILED,
    SEND_QUERY_FAILED,
    SEND_QUERY_PARAMS_FAILED,
    SEND_QUERY_PREPARED_FAILED,
    SEND_PREPARE_FAILED,
    RESULT_FATAL_ERROR,
    RESULT_BAD_RESPONSE
};

class PQErrorCategory
    : public boost::system::error_category
{
public:
    const char* name() const noexcept override
    {
        return "PostgreSQL error category";
    }

    std::string message(int ev) const override
    {
        switch (PQError(ev))
        {
        case PQError::OK:
            return "OK";
        case PQError::CONN_ALLOC_FAILED:
            return "PostgreSQL connection allocation failed";
        case PQError::CONN_INVALID_SOCKET:
            return "PostgreSQL invalid socket handle";
        case PQError::CONN_POLL_FAILED:
            return "PostgreSQL PQconnectPoll failed";
        case PQError::CONSUME_INPUT_FAILED:
            return "PostgreSQL PQconsumeInput failed";
        case PQError::SEND_QUERY_FAILED:
            return "PostgreSQL PQsendQuery failed";
        case PQError::SEND_QUERY_PARAMS_FAILED:
            return "PostgreSQL PQsendQueryParams failed";
        case PQError::SEND_QUERY_PREPARED_FAILED:
            return "PostgreSQL PQsendQueryPrepared failed";
        case PQError::SEND_PREPARE_FAILED:
            return "PostgreSQL PQsendPrepare failed";
        case PQError::RESULT_FATAL_ERROR:
            return "PostgreSQL PQresultStatus: PGRES_FATAL_ERROR";
        case PQError::RESULT_BAD_RESPONSE:
            return "PostgreSQL PQresultStatus: PGRES_BAD_RESPONSE";
        default:
            assert(!"Unexpected PQError value");
            return "Unknown PostgreSQL error";
        }
    }
};

inline const boost::system::error_category& pqcategory()
{
    static PQErrorCategory instance;
    return instance;
}

inline boost::system::error_code make_error_code(PQError e)
{
    return boost::system::error_code{ static_cast<int>(e), pqcategory() };
}

inline boost::system::error_condition make_error_condition(PQError e)
{
    return boost::system::error_condition{ static_cast<int>(e), pqcategory() };
}

} // namespace asiopq
} // namespace ba

namespace boost {
namespace system {

template<>
struct is_error_code_enum<ba::asiopq::PQError>
    : std::true_type
{
};

} // namespace system
} // namespace boost
