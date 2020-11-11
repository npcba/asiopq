#pragma once

#include <type_traits>
#include <boost/system/error_code.hpp>
#include <libpq-fe.h>

namespace ba {
namespace asiopq {

enum class PQError : int
{
    OK = 0,
    CONNECT_POLL_FAILED
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
        case int(PQError::CONNECT_POLL_FAILED):
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
