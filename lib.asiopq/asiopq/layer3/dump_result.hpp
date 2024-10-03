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
        m_opt.fieldSep = const_cast<char*>(", ");
    }

    boost::system::error_code operator()(const ::PGresult* res) const noexcept
    {
        if (!res) // конец данных
            return {};

        switch (::PQresultStatus(res))
        {
        case PGRES_BAD_RESPONSE:
            fprintf(m_fout, "%s\n", ::PQresultErrorMessage(res));
            return make_error_code(PQError::RESULT_BAD_RESPONSE);

        case PGRES_FATAL_ERROR:
            fprintf(m_fout, "%s", ::PQresultErrorMessage(res));
            return make_error_code(PQError::RESULT_FATAL_ERROR);

        default:
            ::PQprint(m_fout, res, &m_opt);
            return {}; // нет ошибки
        }
    }

private:
    FILE* m_fout {nullptr};
    PQprintOpt m_opt = {};
};

} // namespace asiopq
} // namespace ba
