#pragma once

#include <libpq-fe.h>

namespace ba {
namespace asiopq {

class NullParams
{
public:
    constexpr int n() const noexcept
    {
        return 0;
    }

    constexpr const Oid* types() const noexcept
    {
        return nullptr;
    }

    constexpr const char* const* values() const noexcept
    {
        return nullptr;
    }

    constexpr const int* lengths() const noexcept
    {
        return nullptr;
    }

    constexpr const int* formats() const noexcept
    {
        return nullptr;
    }
};

} // namespace ba
} // namespace asiopq
