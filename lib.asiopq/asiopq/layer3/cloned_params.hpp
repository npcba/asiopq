#pragma once

#include <vector>
#include <string>
#include <algorithm>

#include <libpq-fe.h>

#include "../layer2/params.hpp"

namespace ba {
namespace asiopq {

class ClonedParams
{
public:
    template <typename SourceParams>
    ClonedParams(const SourceParams& source)
    {
        const auto n = std::size_t(source.n());
        if (0 == n) // пустые параметры
            return;

        {
            const Oid* const types = source.types();
            if (types)
                m_types.assign(types, types + n);
        }

        {
            const int* const lengths = source.lengths();
            if (lengths)
                m_lengths.assign(lengths, lengths + n);
        }

        {
            const int* const formats = source.formats();
            if (formats)
                m_formats.assign(formats, formats + n);
        }

        const char* const* values = source.values();

        if (m_formats.empty()) // все значения текстовые
        {
            m_values.assign(values, values + n);
        }
        else
        {
            m_values.reserve(n);
            for (std::size_t i = 0; n != i; ++i)
            {
                if (0 == m_formats[i]) // текстовый
                {
                    m_values.emplace_back(values[i]);
                }
                else // двоичный
                {
                    assert(!m_lengths.empty());
                    m_values.emplace_back(values[i], values[i] + m_lengths[i]);
                }
            }
        }

        m_valuesView.reserve(n);
        std::transform(m_values.begin(), m_values.end(), std::back_inserter(m_valuesView), [](const std::string& s) { return s.c_str(); });
    }

    int n() const noexcept
    {
        return int(m_values.size());
    }

    const Oid* types() const noexcept
    {
        return m_types.data();
    }

    const char* const* values() const noexcept
    {
        return m_valuesView.data();
    }

    const int* lengths() const noexcept
    {
        return m_lengths.data();
    }

    const int* formats() const noexcept
    {
        return m_formats.data();
    }

private:
    std::vector<Oid> m_types;
    std::vector<std::string> m_values;
    std::vector<int> m_lengths;
    std::vector<int> m_formats;
    std::vector<const char*> m_valuesView;
};

template <>
struct ParamsTraits<ClonedParams>
{
    using IsOwner = std::true_type;
};

} // namespace ba
} // namespace asiopq
