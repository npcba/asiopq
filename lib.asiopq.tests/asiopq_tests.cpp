#define BOOST_TEST_MODULE LibAsioPQ

#define BOOST_COROUTINE_NO_DEPRECATION_WARNING
#define BOOST_COROUTINES_NO_DEPRECATION_WARNING

#include <functional>
#include <thread>

#include <boost/asio/spawn.hpp>
#include <boost/test/included/unit_test.hpp>
#include <asiopq/async_query.hpp>
#include <asiopq/auto_prepared_query.hpp>
#include <asiopq/text_params.hpp>

class Tester
{
    Tester(const Tester&) = delete;
    Tester operator=(const Tester&) = delete;

public:
    explicit Tester(boost::asio::io_service& ios)
        : m_conn{ ios }
        , m_query{ m_conn, "insert into teledata (foo, bar) VALUES($1, $2)" }
    {
    }

    void start()
    {
        m_conn.asyncConnect("postgresql://postgres:postgres@localhost/egts", std::bind(&Tester::handle, this, std::placeholders::_1));
    }

    void handle(const boost::system::error_code& ec)
    {
        if (!(m_count--))
            return;

        m_query(ba::asiopq::makeTextParamsView("teststringdata1", "teststringdata2"), std::bind(&Tester::handle, this, std::placeholders::_1));
    }

private:
    ba::asiopq::Connection m_conn;
    ba::asiopq::AutoPreparedQuery<> m_query;
    std::size_t m_count = 25000;
};

void test(boost::asio::io_service& ios, boost::asio::yield_context yield)
{
    //boost::asio::ip::tcp::endpoint ep{ boost::asio::ip::address::from_string("127.0.0.1"), 5162 };
    //boost::asio::ip::tcp::socket s{ ios };
    //boost::system::error_code ec;
    //s.async_connect(ep, [](boost::system::error_code ec) {
    //    auto a = 1; });
    //auto size = boost::asio::async_write(s, boost::asio::buffer("1"), boost::asio::transfer_exactly(1), yield);

    ba::asiopq::Connection conn{ ios };
    conn.asyncConnect("postgresql://ctest:ctest@localhost/ctest", yield);
    ba::asiopq::asyncQuery(conn, "CREATE TABLE asiopq(foo text, bar text, baz real)", yield);
    ba::asiopq::AutoPreparedQuery<> query{ conn, "insert into asiopq (foo, bar, baz) VALUES($1, $2, $3)" };

    for (int i = 0; i < 1000; ++i)
    {
        query(ba::asiopq::makeTextParamsView("teststringdata1", "teststringdata2", "0.1"), yield);
    }
}

BOOST_AUTO_TEST_CASE(test_connect)
{
    boost::asio::io_service ioService{ 8 };
    ba::asiopq::Connection conn1{ ioService };
    //conn1.asyncConnect("postgresql://postgres:postgres@localhost/egts", Handler{});
    //ioService.run();

    /*std::vector<std::unique_ptr<Tester>> testers;
    for (int i = 0; i < 40; ++i)
    {
        testers.emplace_back(std::make_unique<Tester>(ioService));
        testers.back()->start();
    }*/

    for (int i = 0; i < 10; ++i)
    {
        boost::asio::spawn( ioService, [&ioService](boost::asio::yield_context yield){
            test(ioService, yield);
        });
    }

    std::vector<std::thread> thrs;
    for (int i = 0; i < 4; ++i)
    {
        thrs.emplace_back([&ioService] {
            ioService.run();
            });
    }

    for (auto& thr : thrs)
    {
        if (thr.joinable())
            thr.join();
    }

    /*::ExecStatusType status = ::PQresultStatus(res);
        PQprintOpt opt = { 0 };
        opt.header = 1;
        opt.align = 1;
        //opt.expanded = 1;
        opt.fieldSep = ", ";
        ::PQprint(stdout, res, &opt);
    }
    }); */
}
