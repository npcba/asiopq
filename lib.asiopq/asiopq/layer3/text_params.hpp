#pragma once

#include <array>
#include <algorithm>
#include <type_traits>
#include <memory>

#include <boost/optional.hpp>

#include <libpq-fe.h>

#include "../layer2/params.hpp"

namespace ba {
namespace asiopq {

template <std::size_t length>
class TextParamsView
{
public:
    // from variadic arguments
    template <typename... Char>
    TextParamsView(const Char*... params) noexcept
        : m_params{ checkedChar(params)... }
    {
        static_assert(sizeof...(params) == length, "Constructor argument count should be equal to 'length' template parameter");
    }

    // from static array
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

// from variadic argumnts
template <typename... Char>
TextParamsView<sizeof...(Char)> makeTextParamsView(const Char*... params)
{
    return { params... };
}

// from static array
template <std::size_t length>
TextParamsView<length> makeTextParamsView(const char* const(&params)[length])
{
    return { params };
}


class TextParams
{
    struct IData
    {
        virtual ~IData() = default;
        virtual int n() const noexcept = 0;
        virtual const char* const* values() const noexcept = 0;
    };

    template <std::size_t length>
    struct Data final : IData
    {
        Data(std::vector<boost::optional<std::string>>&& values, const char* const(&pointers)[length])
            : m_values{ std::move(values) }, m_view{ pointers }
        {
        }

        ~Data() override = default;

        int n() const noexcept override
        {
            return m_view.n();
        }
        virtual const char* const* values() const noexcept override
        {
            return m_view.values();
        }

    private:
        const std::vector<boost::optional<std::string>> m_values;
        const TextParamsView<length> m_view;
    };

public:
    // from variadic arguments
    template <typename... String>
    TextParams(String&&... params)
    {
        std::vector<boost::optional<std::string>> values{ checkedString(std::forward<String>(params))... };
        const char* pointers[sizeof...(params)];

        std::transform(
              values.begin()
            , values.end()
            , std::begin(pointers)
            , [](const boost::optional<std::string>& s) { return (s ? s->c_str() : nullptr); }
            );

        m_data = std::make_shared<Data<sizeof...(params)>>(std::move(values), pointers);
    }

    int n() const noexcept
    {
        return m_data->n();
    }

    const Oid* types() const noexcept
    {
        return nullptr;
    }

    const char* const* values() const noexcept
    {
        return m_data->values();
    }

    const int* lengths() const noexcept
    {
        return nullptr;
    }

    const int* formats() const noexcept
    {
        return nullptr;
    }

private:
    template <typename String>
    static boost::optional<std::string> checkedString(String&& param)
    {
        static_assert(std::is_convertible<String, std::string>::value, "Only std::string-convertible parameters are allowed");
        return boost::optional<std::string>{ std::forward<String>(param) };
    }

    static boost::optional<std::string> checkedString(boost::optional<std::string> param)
    {
        return param;
    }

    static boost::optional<std::string> checkedString(const char* param)
    {
        if (nullptr == param)
            return {}; // значение отсутствует, будет подставлено nullptr в TextParams

        return boost::optional<std::string>{ param };
    }

private:
    std::shared_ptr<IData> m_data;
};

template<>
struct ParamsTraits<TextParams>
{
    using IsOwner = std::true_type;
};

} // namespace ba
} // namespace asiopq
