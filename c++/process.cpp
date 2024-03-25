#include <string>
#include <fstream>
#include <sstream>
#include <iostream>
#include <set>
#include "parser.cpp"
#include "db.cpp"

struct
{
    char k[32];
    long v;
} citem;
long unix_time(const char *timestr)
{
    if (strcmp(citem.k, timestr) == 0)
    {
        return citem.v;
    }
    struct tm t = {};
    strptime(timestr, "%d/%b/%Y:%H:%M:%S", &t);
    long x = mktime(&t);
    strcpy(citem.k, timestr);
    citem.v = x;
    return x;
}

// out 空间至少有32字节
static inline void byteFormat(unsigned long s, char *out)
{
    char const *unit = "KMGTPEZY";
    if (s < 1024)
    {
        snprintf(out, 32, "%lu B", s);
        return;
    }
    unit--;
    double n = (double)s;
    while (n >= 1024)
    {
        n /= 1024;
        unit++;
    }
    snprintf(out, 32, "%.2f %cB", n, *unit);
}

int process(istream &fh)
{
    auto c = dbutil();
    int res = c.begin();
    CHECK(res);

    char str[8192] = {0};

    unsigned long total_bytes_sent = 0;
    unsigned long total_bytes_recv = 0;
    unsigned int total_lines = 0;
    char remote_addr[1024] = {0};
    char remote_user[1024] = {0};
    char request_line[8192] = {0};
    char http_referer[4096] = {0};
    char http_user_agent[4096] = {0};
    char http_x_forwarded_for[1024] = {0};
    char host[1024] = {0};
    char upstream_addr[1024] = {0};

    char value[1024] = {0}; // 后面多处使用此内存池复用

    while (fh.getline(str, sizeof(str)))
    {
        auto a = Line(str);
        if (a.parse_remote_addr(remote_addr) < 0)
        {
            cerr << str << endl;
            continue;
        }

        if (a.parse_remote_user(remote_user) < 0)
        {
            cerr << str << endl;
            continue;
        }

        if (a.parse_time_local(value) < 0)
        {
            cerr << str << endl;
            continue;
        }
        long time_local = unix_time(value);

        if (a.parse_request_line(request_line) < 0)
        {
            cerr << str << endl;
            continue;
        }

        if (a.parse_status_code(value) < 0)
        {
            cerr << str << endl;
            continue;
        }
        int status_code = atoi(value);

        if (a.parse_body_bytes_sent(value) < 0)
        {
            cerr << str << endl;
            continue;
        }
        int body_bytes_sent = atoi(value);

        if (a.parse_http_referer(http_referer) < 0)
        {
            cerr << str << endl;
            continue;
        }

        if (a.parse_http_user_agent(http_user_agent) < 0)
        {
            cerr << str << endl;
            continue;
        }

        if (a.parse_http_x_forwarded_for(http_x_forwarded_for) < 0)
        {
            cerr << str << endl;
            continue;
        }

        if (a.parse_host(host) < 0)
        {
            cerr << str << endl;
            continue;
        }

        if (a.parse_request_length(value) < 0)
        {
            cerr << str << endl;
            continue;
        }
        int request_length = atoi(value);

        if (a.parse_bytes_sent(value) < 0)
        {
            cerr << str << endl;
            continue;
        }
        int bytes_sent = atoi(value);

        if (a.parse_upstream_addr(upstream_addr) < 0)
        {
            cerr << str << endl;
            continue;
        }

        if (a.parse_upstream_status(value) < 0)
        {
            cerr << str << endl;
            continue;
        }
        int upstream_status = atoi(value);

        if (a.parse_request_time(value) < 0)
        {
            cerr << str << endl;
            continue;
        }
        double request_time = atof(value);

        if (a.parse_upstream_response_time(value) < 0)
        {
            cerr << str << endl;
            continue;
        }
        double upstream_response_time = atof(value);

        if (a.parse_upstream_connect_time(value) < 0)
        {
            cerr << str << endl;
            continue;
        }
        double upstream_connect_time = atof(value);

        if (a.parse_upstream_header_time(value) < 0)
        {
            cerr << str << endl;
            continue;
        }
        double upstream_header_time = atof(value);

        // 这一行 所有都已正确解析
        total_lines++;
        total_bytes_sent += bytes_sent;
        total_bytes_recv += request_length;
        c.insert_log(time_local, remote_addr, remote_user, request_line, status_code, body_bytes_sent, http_referer, http_user_agent, http_x_forwarded_for, host, request_length, bytes_sent, upstream_addr, upstream_status, request_time, upstream_response_time, upstream_connect_time, upstream_header_time);
    }
    char b1[64] = {0};
    char b2[64] = {0};
    byteFormat(total_bytes_sent, b1);
    byteFormat(total_bytes_recv, b2);
    printf("共处理%d行,发送数据%s,接收数据%s\n", total_lines, b1, b2);
    return c.end();
}

int query(const char *db, const char *sql)
{
    return db_query(db, sql);
}