#define BOOST_TEST_MODULE LibAsioPQ

#define BOOST_COROUTINE_NO_DEPRECATION_WARNING
#define BOOST_COROUTINES_NO_DEPRECATION_WARNING

#include <thread>

#include <boost/asio/spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/test/included/unit_test.hpp>
#include <asiopq/async_query.hpp>
#include <asiopq/auto_prepared_query.hpp>
#include <asiopq/text_params.hpp>
#include <asiopq/connection_pool.hpp>
#include <asiopq/dump_result.hpp>

const char* const CONNECTION_STRING = "postgresql://ctest:ctest@localhost/ctest";

void connectCoro(boost::asio::io_service& ios, boost::asio::yield_context yield)
{
    ba::asiopq::Connection conn{ ios };
    BOOST_CHECK_NO_THROW(conn.asyncConnect(CONNECTION_STRING, yield));
    BOOST_CHECK(conn.close() == boost::system::error_code{});
}

void createTableCoro(boost::asio::io_service& ios, boost::asio::yield_context yield)
{
    ba::asiopq::Connection conn{ ios };
    conn.asyncConnect(CONNECTION_STRING, yield);
    BOOST_CHECK_NO_THROW(ba::asiopq::asyncQuery(conn, "CREATE TABLE IF NOT EXISTS asiopq(foo text, bar text)", yield));

    // ошибка повторного создания таблицы
    BOOST_CHECK_THROW(ba::asiopq::asyncQuery(conn, "CREATE TABLE asiopq(foo text, bar text)", yield), boost::system::system_error);
    // то же самое, то через error code
    boost::system::error_code ec;
    ba::asiopq::asyncQuery(conn, "CREATE TABLE asiopq(foo text, bar text)", yield[ec]);
    BOOST_CHECK(ba::asiopq::PQError::RESULT_FATAL_ERROR == ec);
}

void insertCoro(boost::asio::io_service& ios, boost::asio::yield_context yield)
{
    ba::asiopq::Connection conn{ ios };
    conn.asyncConnect(CONNECTION_STRING, yield);
    ba::asiopq::AutoPreparedQuery<> query{ conn, "insert into asiopq (foo, bar) VALUES($1, $2)" };

    for (int i = 0; i < 1000; ++i)
        query(ba::asiopq::makeTextParamsView("teststringdata1", "teststringdata2"), yield);
}

BOOST_AUTO_TEST_CASE(connectTest)
{
    boost::asio::io_service ios;
    boost::asio::spawn(ios, [&ios](boost::asio::yield_context yield) {
        connectCoro(ios, yield);
        });

    ios.run();
}

BOOST_AUTO_TEST_CASE(createTableTest)
{
    boost::asio::io_service ios;
    boost::asio::spawn(ios, [&ios](boost::asio::yield_context yield) {
        createTableCoro(ios, yield);
        });

    ios.run();
}

BOOST_AUTO_TEST_CASE(insertTest)
{
    boost::asio::io_service ios{ 4 };

    for (int i = 0; i < 10; ++i)
        boost::asio::spawn(ios, [&ios](boost::asio::yield_context yield){
            insertCoro(ios, yield);
        });

    std::vector<std::thread> thrs;
    for (int i = 0; i < 4; ++i)
        thrs.emplace_back([&ios] {
            ios.run();
            });

    for (auto& thr : thrs)
    {
        if (thr.joinable())
            thr.join();
    }
}

BOOST_AUTO_TEST_CASE(deleteUseFutureTest)
{
    boost::asio::io_service ios;
    ba::asiopq::Connection conn{ ios };
    std::future<void> connected = conn.asyncConnect(CONNECTION_STRING, boost::asio::use_future);
    ios.run();
    BOOST_CHECK_NO_THROW(connected.get());

    ios.reset();
    std::future<void> deleted = ba::asiopq::asyncQuery(conn, "DELETE FROM asiopq", boost::asio::use_future);
    ios.run();
    BOOST_CHECK_NO_THROW(deleted.get());

    /*ios.reset();
    std::future<void> dropped = ba::asiopq::asyncQuery(conn, "DROP TABLE asiopq", boost::asio::use_future);
    ios.run();
    BOOST_CHECK_NO_THROW(dropped.get());

    // повторный DROP должен вызвать исключение
    ios.reset();
    dropped = ba::asiopq::asyncQuery(conn, "DROP TABLE asiopq", boost::asio::use_future);
    ios.run();
    BOOST_CHECK_THROW(dropped.get(), boost::system::system_error);*/
}

BOOST_AUTO_TEST_CASE(connectionPool)
{
    boost::asio::io_service ios;
    ba::asiopq::ConnectionPool pool{ ios, 10 };

    auto op = [](ba::asiopq::Connection& conn, auto handler)
    {
        ba::asiopq::asyncQuery(conn, "insert into asiopq (foo, bar) VALUES('a', 'b')", handler, ba::asiopq::IgnoreResult{});
    };
    auto handler = [](const boost::system::error_code& ec) {

    };

    for (int i = 0; i < 10000; ++i)
        pool.exec(op, handler);

    ios.run();
}
