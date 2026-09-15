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

static inline int sv_to_int(const char *str)
{
    int result = 0;
    while (*str >= '0' && *str <= '9')
    {
        result = result * 10 + (*str - '0');
        str++;
    }
    return result;
}

static inline double sv_to_double(const char *str)
{
    long long int_part = 0;
    while (*str >= '0' && *str <= '9')
    {
        int_part = int_part * 10 + (*str - '0');
        str++;
    }
    if (*str == '.')
    {
        str++;
        long long frac = 0;
        long long scale = 1;
        while (*str >= '0' && *str <= '9' && scale < 1000000000000LL)
        {
            frac = frac * 10 + (*str - '0');
            scale *= 10;
            str++;
        }
        return (double)int_part + (double)frac / (double)scale;
    }
    return (double)int_part;
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

int process(FILE *fh)
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

    while (fgets(str, sizeof(str), fh))
    {
        auto a = Line(str);
        if (a.parse_remote_addr(remote_addr) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }

        if (a.parse_remote_user(remote_user) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }

        if (a.parse_time_local(value) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }
        long time_local = unix_time(value);

        if (a.parse_request_line(request_line) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }

        if (a.parse_status_code(value) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }
        int status_code = sv_to_int(value);

        if (a.parse_body_bytes_sent(value) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }
        int body_bytes_sent = sv_to_int(value);

        if (a.parse_http_referer(http_referer) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }

        if (a.parse_http_user_agent(http_user_agent) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }

        if (a.parse_http_x_forwarded_for(http_x_forwarded_for) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }

        if (a.parse_host(host) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }

        if (a.parse_request_length(value) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }
        int request_length = sv_to_int(value);

        if (a.parse_bytes_sent(value) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }
        int bytes_sent = sv_to_int(value);

        if (a.parse_upstream_addr(upstream_addr) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }

        if (a.parse_upstream_status(value) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }
        int upstream_status = sv_to_int(value);

        if (a.parse_request_time(value) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }
        double request_time = sv_to_double(value);

        if (a.parse_upstream_response_time(value) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }
        double upstream_response_time = sv_to_double(value);

        if (a.parse_upstream_connect_time(value) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }
        double upstream_connect_time = sv_to_double(value);

        if (a.parse_upstream_header_time(value) < 0)
        {
            std::cerr << str << std::endl;
            continue;
        }
        double upstream_header_time = sv_to_double(value);

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