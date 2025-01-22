#include "sqlite3.h"
#include <string>
#include <cstring>
#include <fstream>
#include <sstream>
#include <iostream>

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
            "CREATE TABLE 'nginx_log' ( 'id' integer NOT NULL PRIMARY KEY AUTOINCREMENT, 'time' integer NOT NULL, 'remote_addr' text NOT NULL, 'remote_user' text NOT NULL, 'request' text NOT NULL, 'status' integer NOT NULL, 'body_bytes_sent' integer NOT NULL, 'http_referer' text NOT NULL, 'http_user_agent' text NOT NULL, 'http_x_forwarded_for' text NOT NULL, 'host' text NOT NULL, 'request_length' integer NOT NULL, 'bytes_sent' integer NOT NULL, 'upstream_addr' text NOT NULL, 'upstream_status' integer NOT NULL, 'request_time' real NOT NULL, 'upstream_response_time' real NOT NULL, 'upstream_connect_time' real NOT NULL, 'upstream_header_time' real NOT NULL ) STRICT;"
            "CREATE INDEX 'log_time' ON 'nginx_log' ('time');";
        if (sqlite3_exec(this->db, sql, NULL, NULL, &err_msg) != SQLITE_OK)
        {
            sqlite3_close(this->db);
            throw err_msg;
        }
        sql = "INSERT INTO `nginx_log`(time,remote_addr,remote_user,request,status,body_bytes_sent,http_referer,http_user_agent,http_x_forwarded_for,host,request_length,bytes_sent,upstream_addr,upstream_status,request_time,upstream_response_time,upstream_connect_time,upstream_header_time) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
        if (sqlite3_prepare_v2(this->db, sql, strlen(sql), &stmt_log, NULL) != SQLITE_OK)
        {
            sqlite3_close(this->db);
            throw sqlite3_errmsg(this->db);
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
    int insert_log(long time, const char *remote_addr, const char *remote_user, const char *request, int status, int body_bytes_sent, const char *http_referer, const char *http_user_agent, const char *http_x_forwarded_for, const char *host, int request_length, int bytes_sent, const char *upstream_addr, int upstream_status, double request_time, double upstream_response_time, double upstream_connect_time, double upstream_header_time)
    {
        CHECKZERO(sqlite3_bind_int(stmt_log, 1, time));
        CHECKZERO(sqlite3_bind_text(stmt_log, 2, remote_addr, -1, SQLITE_STATIC));
        CHECKZERO(sqlite3_bind_text(stmt_log, 3, remote_user, -1, SQLITE_STATIC));
        CHECKZERO(sqlite3_bind_text(stmt_log, 4, request, -1, SQLITE_STATIC));
        CHECKZERO(sqlite3_bind_int(stmt_log, 5, status));
        CHECKZERO(sqlite3_bind_int(stmt_log, 6, body_bytes_sent));
        CHECKZERO(sqlite3_bind_text(stmt_log, 7, http_referer, -1, SQLITE_STATIC));
        CHECKZERO(sqlite3_bind_text(stmt_log, 8, http_user_agent, -1, SQLITE_STATIC));
        CHECKZERO(sqlite3_bind_text(stmt_log, 9, http_x_forwarded_for, -1, SQLITE_STATIC));
        CHECKZERO(sqlite3_bind_text(stmt_log, 10, host, -1, SQLITE_STATIC));
        CHECKZERO(sqlite3_bind_int(stmt_log, 11, request_length));
        CHECKZERO(sqlite3_bind_int(stmt_log, 12, bytes_sent));
        CHECKZERO(sqlite3_bind_text(stmt_log, 13, upstream_addr, -1, SQLITE_STATIC));
        CHECKZERO(sqlite3_bind_int(stmt_log, 14, upstream_status));
        CHECKZERO(sqlite3_bind_double(stmt_log, 15, request_time));
        CHECKZERO(sqlite3_bind_double(stmt_log, 16, upstream_response_time));
        CHECKZERO(sqlite3_bind_double(stmt_log, 17, upstream_connect_time));
        CHECKZERO(sqlite3_bind_double(stmt_log, 18, upstream_header_time));
        CHECKZERO(SQLITE_DONE - sqlite3_step(stmt_log));
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

int sqlite_exec_cb(void *data, int ncols, char **values, char **attribute)
{
    for (int i = 0; i < ncols; i++)
    {
        (i > 0) ? printf("|%s", values[i]) : printf("%s", values[i]);
    }
    printf("\n");
    return 0;
}

int db_query(const char *file, const char *sql)
{
    sqlite3 *db;
    if (sqlite3_open_v2(file, &db, SQLITE_OPEN_READONLY, NULL))
    {
        throw sqlite3_errmsg(db);
    }
    char *err_msg;
    if (sqlite3_exec(db, sql, sqlite_exec_cb, NULL, &err_msg) != SQLITE_OK)
    {
        sqlite3_close_v2(db);
        throw err_msg;
    }
    return sqlite3_close_v2(db);
}
