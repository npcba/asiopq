#pragma once

#include <libpq-fe.h>

#include <boost/system/error_code.hpp>


namespace ba {
namespace asiopq {

class DumpResult
{
public:
    explicit DumpResult(const PQprintOpt& opt, FILE* fout = stdout)
        : m_fout{ fout }
        , m_opt{ opt }
    {
    }

    explicit DumpResult(FILE* fout = stdout)
        : m_fout{ fout }
    {
        m_opt.header = 1;
        m_opt.align = 1;
        m_opt.expanded = 1;
        m_opt.fieldSep = ", ";
    }

    boost::system::error_code operator()(const ::PGresult* res) const noexcept
    {
        if (!res) // конец данных
            return {};

        const auto status = ::PQresultStatus(res);
        if (PGRES_FATAL_ERROR == status)
        {
            fprintf(m_fout, "%s", ::PQresultErrorMessage(res));
            return make_error_code(PQError::RESULT_FATAL_ERROR);
        }
        if (PGRES_BAD_RESPONSE == status)
        {
            fprintf(m_fout, "%s\n", ::PQresultErrorMessage(res));
            return make_error_code(PQError::RESULT_BAD_RESPONSE);
        }

        ::PQprint(m_fout, res, &m_opt);

        return {};
    }

private:
    FILE* m_fout = nullptr;
    PQprintOpt m_opt = {0};
};

} // namespace asiopq
} // namespace ba
