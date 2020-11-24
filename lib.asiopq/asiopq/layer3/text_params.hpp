#pragma once

#include <array>
#include <algorithm>
#include <type_traits>

#include <libpq-fe.h>

#include "../layer2/params.hpp"

namespace ba {
namespace asiopq {

template <std::size_t length>
class TextParamsView
{
public:
    template <typename... Char>
    TextParamsView(const Char*... params...) noexcept
        : m_params{ checkedChar(params)... }
    {
        static_assert(sizeof...(params) == length, "Constructor argument count should be equal to 'length' template parameter");
    }

    TextParamsView(const char* const(&params)[length]) noexcept
    {
        std::copy(std::begin(params), std::end(params), m_params.begin());
    }

    constexpr int n() const noexcept
    {
        return int(m_params.size());
    }

    constexpr const Oid* types() const noexcept
    {
        return nullptr;
    }

    const char* const* values() const noexcept
    {
        return m_params.data();
    }

    constexpr const int* lengths() const noexcept
    {
        return nullptr;
    }

    constexpr const int* formats() const noexcept
    {
        return nullptr;
    }

private:
    template <typename Char>
    static constexpr const char* checkedChar(const Char* param) noexcept
    {
        static_assert(std::is_same<Char, char>::value, "Only const char* parameters are allowed");
        return param;
    }

private:
    std::array<const char*, length> m_params;
};

template <std::size_t length>
struct ParamsTraits<TextParamsView<length>>
{
    using IsOwner = std::false_type;
};

template <typename... Char>
TextParamsView<sizeof...(Char)> makeTextParamsView(const Char*... params...)
{
    return { params... };
}

template <std::size_t length>
TextParamsView<length> makeTextParamsView(const char* const(&params)[length])
{
    return { params };
}

} // namespace ba
} // namespace asiopq
