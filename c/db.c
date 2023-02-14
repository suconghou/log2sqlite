#include <stdio.h>
#include <string.h>
#include "sqlite3.h"

#define CHECK(a)        \
    if (a != SQLITE_OK) \
    return a

sqlite3 *pDB;
sqlite3_stmt *stmt_log;

int db_prepare()
{
    if (sqlite3_open("nginx_log.db", &pDB))
    {
        fprintf(stderr, "open error: %s\n", sqlite3_errmsg(pDB));
        return 1;
    }
    // 创建表
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
    int res = sqlite3_exec(pDB, sql, NULL, NULL, &err_msg);
    if (res != SQLITE_OK)
    {
        fprintf(stderr, "sql error: %s\n", err_msg);
        sqlite3_free(err_msg);
        sqlite3_close(pDB);
        return res;
    }
    // 预处理缓存下来
    sql = "INSERT INTO `nginx_log`(time,remote_addr,remote_user,request,status,body_bytes_sent,http_referer,http_user_agent,http_x_forwarded_for,host,request_length,bytes_sent,upstream_addr,upstream_status,request_time,upstream_response_time,upstream_connect_time,upstream_header_time) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
    res = sqlite3_prepare_v2(pDB, sql, strlen(sql), &stmt_log, NULL);
    if (res != SQLITE_OK)
    {
        fprintf(stderr, "prepare error: %s\n", sqlite3_errmsg(pDB));
        sqlite3_close(pDB);
        return res;
    }
    return 0;
}

int db_begin()
{
    char *err_msg;
    int res = sqlite3_exec(pDB, "BEGIN;", NULL, NULL, &err_msg);
    if (res != SQLITE_OK)
    {
        fprintf(stderr, "begin transaction error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return res;
    }
    return 0;
}

int db_insert(long time, char *remote_addr, char *remote_user, char *request, int status, int body_bytes_sent, char *http_referer, char *http_user_agent, char *http_x_forwarded_for, char *host, int request_length, int bytes_sent, char *upstream_addr, int upstream_status, double request_time, double upstream_response_time, double upstream_connect_time, double upstream_header_time)
{
    int res = sqlite3_bind_int(stmt_log, 1, time);
    CHECK(res);
    res = sqlite3_bind_text(stmt_log, 2, remote_addr, strlen(remote_addr), NULL);
    CHECK(res);
    res = sqlite3_bind_text(stmt_log, 3, remote_user, strlen(remote_user), NULL);
    CHECK(res);
    res = sqlite3_bind_text(stmt_log, 4, request, strlen(request), NULL);
    CHECK(res);
    res = sqlite3_bind_int(stmt_log, 5, status);
    CHECK(res);
    res = sqlite3_bind_int(stmt_log, 6, body_bytes_sent);
    CHECK(res);
    res = sqlite3_bind_text(stmt_log, 7, http_referer, strlen(http_referer), NULL);
    CHECK(res);
    res = sqlite3_bind_text(stmt_log, 8, http_user_agent, strlen(http_user_agent), NULL);
    CHECK(res);
    res = sqlite3_bind_text(stmt_log, 9, http_x_forwarded_for, strlen(http_x_forwarded_for), NULL);
    CHECK(res);
    res = sqlite3_bind_text(stmt_log, 10, host, strlen(host), NULL);
    CHECK(res);
    res = sqlite3_bind_int(stmt_log, 11, request_length);
    CHECK(res);
    res = sqlite3_bind_int(stmt_log, 12, bytes_sent);
    CHECK(res);
    res = sqlite3_bind_text(stmt_log, 13, upstream_addr, strlen(upstream_addr), NULL);
    CHECK(res);
    res = sqlite3_bind_int(stmt_log, 14, upstream_status);
    CHECK(res);
    res = sqlite3_bind_double(stmt_log, 15, request_time);
    CHECK(res);
    res = sqlite3_bind_double(stmt_log, 16, upstream_response_time);
    CHECK(res);
    res = sqlite3_bind_double(stmt_log, 17, upstream_connect_time);
    CHECK(res);
    res = sqlite3_bind_double(stmt_log, 18, upstream_header_time);
    CHECK(res);
    if (SQLITE_DONE != sqlite3_step(stmt_log))
    {
        return 101;
    }
    res = sqlite3_clear_bindings(stmt_log);
    CHECK(res);
    return sqlite3_reset(stmt_log);
}

int db_end()
{
    char *err_msg;
    int res = sqlite3_exec(pDB, "COMMIT;", NULL, NULL, &err_msg);
    if (res != SQLITE_OK)
    {
        fprintf(stderr, "end transaction error: %s\n", err_msg);
        sqlite3_free(err_msg);
        return res;
    }
    return 0;
}