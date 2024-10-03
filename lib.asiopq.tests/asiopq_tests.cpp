#define BOOST_TEST_MODULE LibAsioPQ

#define BOOST_COROUTINE_NO_DEPRECATION_WARNING
#define BOOST_COROUTINES_NO_DEPRECATION_WARNING

#include <asiopq/async_query.hpp>
#include <asiopq/async_query_params.hpp>
#include <asiopq/auto_prepared_query.hpp>
#include <asiopq/text_params.hpp>
#include <asiopq/reconnection_pool.hpp>
#include <asiopq/dump_result.hpp>

#include <thread>

#include <boost/asio/spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/test/included/unit_test.hpp>

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
    ba::asiopq::AutoPreparedQuery<> query{ conn, "insert into asiopq(foo, bar) VALUES('a', 'b')" };

    for (int i = 0; i < 1'000; ++i)
        ba::asiopq::asyncQuery(conn, "insert into asiopq (foo, bar) VALUES('a', 'b')", yield);
}

void poolCoro(boost::asio::io_service& ios, boost::asio::yield_context yield)
{
    auto connectOp = [](ba::asiopq::Connection& conn, auto&& handler)
    {
        conn.asyncConnect(CONNECTION_STRING, std::forward<decltype(handler)>(handler));
    };
    
    auto queryOp = ba::asiopq::compose([](ba::asiopq::Connection& conn, auto&& handler)
    {
            if (::CONNECTION_OK != ::PQstatus(conn.get()))
            {
                handler(make_error_code(ba::asiopq::PQError::SEND_QUERY_FAILED));
                return;
            }

            ba::asiopq::asyncQuery(conn, "insert into asiopq (foo, bar) VALUES('a', 'b')", std::forward<decltype(handler)>(handler));
    });

    auto op = (queryOp | (connectOp & queryOp));

    ba::asiopq::ConnectionPool<decltype(op), decltype(yield)> pool{ ios, 2 };

    const ba::asiopq::Connection* conn;
    for (int i = 0; i < 1'000; ++i)
        conn = pool(op, yield);
    std::ignore = conn;
}

BOOST_AUTO_TEST_CASE(connectTest)
{
    boost::asio::io_service ios;
    boost::asio::spawn(ios, [&ios](boost::asio::yield_context yield) {
        try {
            connectCoro(ios, yield);
        }
        catch (const std::exception& err) {
            BOOST_ERROR(err.what());
        }

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
    boost::asio::io_service ios;

    for (int i = 0; i < 10; ++i)
        boost::asio::spawn(ios, [&ios](boost::asio::yield_context yield){
            try {
                insertCoro(ios, yield);
            }
            catch (const std::exception& err) {
                BOOST_ERROR(err.what());
            }
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

BOOST_AUTO_TEST_CASE(poolTest)
{
    boost::asio::io_service ios;

    auto queryOp = [](ba::asiopq::Connection& conn, auto&& handler)
    {
        if (::CONNECTION_OK != ::PQstatus(conn.get()))
        {
            handler(make_error_code(ba::asiopq::PQError::SEND_QUERY_FAILED));
            return;
        }

        ba::asiopq::asyncQueryParams(conn, "insert into asiopq (foo, bar) VALUES($1, $2)", ba::asiopq::TextParams{ "a", "b" }, true, std::forward<decltype(handler)>(handler));
    };

    std::atomic_size_t n{ 0 };
    auto handler = [&n](const boost::system::error_code& ec, const ba::asiopq::Connection*) {
        if (!ec)
            ++n;
    };

    const auto pool = ba::asiopq::makeReconnectionPool<decltype(queryOp), decltype(handler)>(ios, 40, CONNECTION_STRING);
    const auto pool2 = ba::asiopq::makeReconnectionPool<decltype(queryOp), decltype(handler)>(ios, 40, ba::asiopq::makeConnectOperation(CONNECTION_STRING));
    const auto pool3 = ba::asiopq::makeReconnectionPool<decltype(queryOp), decltype(handler)>(ios, 40, { {} }, false);


    for (int i = 0; i < 10'000; ++i)
        (*pool)(queryOp, handler);

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

    BOOST_CHECK(10'000 == n);
}

BOOST_AUTO_TEST_CASE(coroPoolTest)
{
    boost::asio::io_service ios;

    for (int i = 0; i < 10; ++i)
        boost::asio::spawn(ios, [&ios](boost::asio::yield_context yield){
            try {
                poolCoro(ios, yield);
            }
            catch (const std::exception& err) {
                BOOST_ERROR(err.what());
            }
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

    ios.reset();
    std::future<void> dropped = ba::asiopq::asyncQuery(conn, "DROP TABLE asiopq", boost::asio::use_future);
    ios.run();
    BOOST_CHECK_NO_THROW(dropped.get());

    // повторный DROP должен вызвать исключение
    ios.reset();
    dropped = ba::asiopq::asyncQuery(conn, "DROP TABLE asiopq", boost::asio::use_future);
    ios.run();
    BOOST_CHECK_THROW(dropped.get(), boost::system::system_error);
}

void connectToExistPortCoro(boost::asio::io_service& ios, boost::asio::yield_context yield)
{
    std::string connString = CONNECTION_STRING;
    ba::asiopq::Connection conn{ ios };

    BOOST_CHECK_NO_THROW(conn.asyncConnect((connString + "?connect_timeout=0").c_str(), yield));
    BOOST_CHECK(conn.close() == boost::system::error_code{});

    BOOST_CHECK_NO_THROW(conn.asyncConnect((connString + "?connect_timeout=-1").c_str(), yield));
    BOOST_CHECK(conn.close() == boost::system::error_code{});

    BOOST_CHECK_NO_THROW(conn.asyncConnect((connString + "?connect_timeout=1").c_str(), yield));
    BOOST_CHECK(conn.close() == boost::system::error_code{});

    BOOST_CHECK_NO_THROW(conn.asyncConnect((connString + "?connect_timeout=2").c_str(), yield));
    BOOST_CHECK(conn.close() == boost::system::error_code{});

    BOOST_CHECK_NO_THROW(conn.asyncConnect((connString + "?connect_timeout=10").c_str(), yield));
    BOOST_CHECK(conn.close() == boost::system::error_code{});
}

void connectToNotExistPortCoro(boost::asio::io_service& ios, boost::asio::yield_context yield)
{
    std::string connString = "postgresql://ctest:ctest@localhost:12345/ctest"; // несуществующий порт
    ba::asiopq::Connection conn{ ios };

    BOOST_CHECK_THROW(conn.asyncConnect((connString + "?connect_timeout=2").c_str(), yield), boost::system::system_error);
    BOOST_CHECK(conn.close() == boost::system::error_code{});
}

BOOST_AUTO_TEST_CASE(connectTimeoutTest)
{
    boost::asio::io_service ios;
    boost::asio::spawn(ios, [&ios](boost::asio::yield_context yield) {
        try {
            connectToExistPortCoro(ios, yield);
        }
        catch (const std::exception& err) {
            BOOST_ERROR(err.what());
        }
        });

    ios.run();
    ios.reset();

    boost::asio::spawn(ios, [&ios](boost::asio::yield_context yield) {
        try {
            connectToNotExistPortCoro(ios, yield);
        }
        catch (const std::exception& err) {
            BOOST_ERROR(err.what());
        }
        });

    ios.run();
}
