#include "sqlite3.h"
#include <string>
#include <fstream>
#include <sstream>
#include <iostream>

using namespace std;

#define CHECK(a)        \
    if (a != SQLITE_OK) \
    return a

#define CHECKZERO(a) \
    if ((a) != 0)    \
        throw("error.");

class dbutil
{

private:
    sqlite3 *db;

    sqlite3_stmt *stmt_log;

public:
    dbutil()
    {
        if (sqlite3_open("nginx_log.db", &this->db))
        {
            throw sqlite3_errmsg(this->db);
        }
        char *err_msg;
        const char *sql =
            "PRAGMA JOURNAL_MODE=MEMORY;"
            "PRAGMA LOCKING_MODE=EXCLUSIVE;"
            "PRAGMA SYNCHRONOUS=OFF;"
            "PRAGMA CACHE_SIZE=8192;"
            "PRAGMA PAGE_SIZE=4096;"
            "PRAGMA TEMP_STORE=MEMORY;"
            "DROP TABLE IF EXISTS 'nginx_log';"
            "CREATE TABLE 'nginx_log' ( 'id' integer NOT NULL PRIMARY KEY AUTOINCREMENT, 'time' integer NOT NULL, 'remote_addr' text NOT NULL, 'remote_user' text NOT NULL, 'request' text NOT NULL, 'status' integer NOT NULL, 'body_bytes_sent' integer NOT NULL, 'http_referer' text NOT NULL, 'http_user_agent' text NOT NULL, 'http_x_forwarded_for' text NOT NULL, 'host' text NOT NULL, 'request_length' integer NOT NULL, 'bytes_sent' integer NOT NULL, 'upstream_addr' text NOT NULL, 'upstream_status' integer NOT NULL, 'request_time' real NOT NULL, 'upstream_response_time' real NOT NULL, 'upstream_connect_time' real NOT NULL, 'upstream_header_time' real NOT NULL );"
            "CREATE INDEX 'log_time' ON 'nginx_log' ('time');";
        if (sqlite3_exec(this->db, sql, NULL, NULL, &err_msg) != SQLITE_OK)
        {
            fprintf(stderr, "sql error: %s\n", err_msg);
            sqlite3_free(err_msg);
            sqlite3_close(this->db);
            throw "init error";
        }
        sql = "INSERT INTO `nginx_log`(time,remote_addr,remote_user,request,status,body_bytes_sent,http_referer,http_user_agent,http_x_forwarded_for,host,request_length,bytes_sent,upstream_addr,upstream_status,request_time,upstream_response_time,upstream_connect_time,upstream_header_time) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        if (sqlite3_prepare_v2(this->db, sql, strlen(sql), &stmt_log, NULL) != SQLITE_OK)
        {
            fprintf(stderr, "prepare error: %s\n", sqlite3_errmsg(this->db));
            sqlite3_close(this->db);
            throw "prepare sql error";
        }
    }

    int begin()
    {
        char *err_msg;
        int res = sqlite3_exec(this->db, "BEGIN;", NULL, NULL, &err_msg);
        if (res != SQLITE_OK)
        {
            fprintf(stderr, "begin transaction error: %s\n", err_msg);
            sqlite3_free(err_msg);
            return res;
        }
        return 0;
    }

    // 如果出错将会抛出错误，否则必然返回0
    int insert_log(long time, string &remote_addr, string &remote_user, string &request, int status, int body_bytes_sent, string &http_referer, string &http_user_agent, string &http_x_forwarded_for, string &host, int request_length, int bytes_sent, string &upstream_addr, int upstream_status, double request_time, double upstream_response_time, double upstream_connect_time, double upstream_header_time)
    {
        CHECKZERO(sqlite3_bind_int(stmt_log, 1, time));
        CHECKZERO(sqlite3_bind_text(stmt_log, 2, remote_addr.c_str(), remote_addr.length(), NULL));
        CHECKZERO(sqlite3_bind_text(stmt_log, 3, remote_user.c_str(), remote_user.length(), NULL));
        CHECKZERO(sqlite3_bind_text(stmt_log, 4, request.c_str(), request.length(), NULL));
        CHECKZERO(sqlite3_bind_int(stmt_log, 5, status));
        CHECKZERO(sqlite3_bind_int(stmt_log, 6, body_bytes_sent));
        CHECKZERO(sqlite3_bind_text(stmt_log, 7, http_referer.c_str(), http_referer.length(), NULL));
        CHECKZERO(sqlite3_bind_text(stmt_log, 8, http_user_agent.c_str(), http_user_agent.length(), NULL));
        CHECKZERO(sqlite3_bind_text(stmt_log, 9, http_x_forwarded_for.c_str(), http_x_forwarded_for.length(), NULL));
        CHECKZERO(sqlite3_bind_text(stmt_log, 10, host.c_str(), host.length(), NULL));
        CHECKZERO(sqlite3_bind_int(stmt_log, 11, request_length));
        CHECKZERO(sqlite3_bind_int(stmt_log, 12, bytes_sent));
        CHECKZERO(sqlite3_bind_text(stmt_log, 13, upstream_addr.c_str(), upstream_addr.length(), NULL));
        CHECKZERO(sqlite3_bind_int(stmt_log, 14, upstream_status));
        CHECKZERO(sqlite3_bind_double(stmt_log, 15, request_time));
        CHECKZERO(sqlite3_bind_double(stmt_log, 16, upstream_response_time));
        CHECKZERO(sqlite3_bind_double(stmt_log, 17, upstream_connect_time));
        CHECKZERO(sqlite3_bind_double(stmt_log, 18, upstream_header_time));
        CHECKZERO(SQLITE_DONE - sqlite3_step(stmt_log));
        CHECKZERO(sqlite3_clear_bindings(stmt_log));
        CHECKZERO(sqlite3_reset(stmt_log)); // 重置绑定
        return 0;
    }

    int end()
    {
        char *err_msg;
        auto res = sqlite3_exec(this->db, "COMMIT;", NULL, NULL, &err_msg);
        if (res != SQLITE_OK)
        {
            fprintf(stderr, "end transaction error: %s\n", err_msg);
            sqlite3_free(err_msg);
            return res;
        }
        return 0;
    }
};