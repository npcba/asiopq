#pragma once

#include <libpq-fe.h>

namespace ba {
namespace asiopq {

struct IgnoreResult
{
    void operator()(::PGresult* res) const noexcept
    {
        /*::ExecStatusType status = ::PQresultStatus(res);
        PQprintOpt opt = { 0 };
        opt.header = 1;
        opt.align = 1;
        //opt.expanded = 1;
        opt.fieldSep = ", ";
        ::PQprint(stdout, res, &opt);*/
    }
};

} // namespace asiopq
} // namespace ba
