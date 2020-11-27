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
    const char* name() const noexcept override
    {
        return "PostgreSQL error category";
    }

    std::string message(int ev) const override
    {
        switch (ev)
        {
        case int(PQError::CONN_POLL_FAILED):
            return "PostgreSQL PQconnectPoll failed";
        default:
            return "Unknown PostgreSQL error";
        }
    }
};

const boost::system::error_category& pqcategory()
{
    static PQErrorCategory instance;
    return instance;
}

boost::system::error_code make_error_code(PQError e)
{
    return boost::system::error_code{ static_cast<int>(e), pqcategory() };
}

boost::system::error_condition make_error_condition(PQError e)
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
