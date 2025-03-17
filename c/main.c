#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include "db.c"

typedef int (*char_is_match)(unsigned char x, unsigned char y);

// 仅数字
int digital(unsigned char x, unsigned char y)
{
    return x >= 48 && x <= 57;
}

// 包含数字和.号
int digital_dot(unsigned char x, unsigned char y)
{
    return (x >= 48 && x <= 57) || x == 46;
}

// 包含数字字母[a-f]和.号或:号（IPv4或IPv6）
int digital_dot_colon(unsigned char x, unsigned char y)
{
    return (x >= 48 && x <= 58) || x == 46 || (x >= 97 && x <= 102);
}

// 包含数字和.号或-号
int digital_dot_minus(unsigned char x, unsigned char y)
{
    return (x >= 48 && x <= 57) || x == 46 || x == 45;
}

// 非空格
int not_space(unsigned char x, unsigned char y)
{
    return x != 32;
}

// 当前是空格，上一个是-或者数字
int digital_or_none_end(unsigned char x, unsigned char y)
{
    return !(x == 32 && ((y >= 48 && y <= 57) || y == 45));
}

int parse_item_trim_space(const char *s, const char **pos_ptr, const char *end, char *item_value, char_is_match cond)
{
    const char *pos = *pos_ptr;
    while (pos < end && *pos == ' ')
    {
        ++pos;
    }
    *pos_ptr = pos;
    const char *found_start = NULL;
    const char *found_end = NULL;
    unsigned char y = (pos > s) ? *(pos - 1) : 0;
    while (pos < end)
    {
        unsigned char x = *pos++;
        if (cond(x, y))
        {
            y = x;
            found_end = pos - 1;
            if (!found_start)
            {
                found_start = found_end;
            }
            if (pos < end)
            {
                continue;
            }
        }
        if (!found_start)
        {
            return -1;
        }
        // cond成立时,则包含当前字符x，否则不包含，截取的字符最少1字节
        const int v_len = found_end - found_start + 1;
        memcpy(item_value, found_start, v_len);
        item_value[v_len] = '\0';
        while (pos < end && *pos == ' ')
        {
            ++pos;
        }
        *pos_ptr = pos;
        return found_start - s;
    }
    return found_start ? (found_start - s) : -1;
}

int parse_item_wrap_string(const char *s, const char **pos_ptr, const char *end, char *item_value, const char left, const char right)
{
    const char *pos = *pos_ptr;
    while (pos < end && *pos == ' ')
    {
        ++pos;
    }
    if (pos >= end || *pos != left)
    {
        return -1;
    }
    ++pos;
    const char *p = memchr(pos, right, end - pos);
    if (!p)
    {
        return -1;
    }
    const int v_len = p - pos;
    memcpy(item_value, pos, v_len);
    item_value[v_len] = '\0';
    *pos_ptr = p + 1;
    return 1;
}

int parse_remote_addr(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_trim_space(s, pos_ptr, end, item_value, digital_dot_colon);
}

int parse_remote_user(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    const char *pos = *pos_ptr;

    while (pos < end && *pos == '-')
    {
        ++pos;
    }

    *pos_ptr = pos;
    return parse_item_trim_space(s, pos_ptr, end, item_value, not_space);
}

int parse_time_local(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_wrap_string(s, pos_ptr, end, item_value, '[', ']');
}

int parse_request_line(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_wrap_string(s, pos_ptr, end, item_value, '"', '"');
}

int parse_status_code(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_trim_space(s, pos_ptr, end, item_value, digital);
}

int parse_body_bytes_sent(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_trim_space(s, pos_ptr, end, item_value, digital);
}

int parse_http_referer(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_wrap_string(s, pos_ptr, end, item_value, '"', '"');
}

int parse_http_user_agent(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_wrap_string(s, pos_ptr, end, item_value, '"', '"');
}

int parse_http_x_forwarded_for(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_wrap_string(s, pos_ptr, end, item_value, '"', '"');
}

int parse_host(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_trim_space(s, pos_ptr, end, item_value, not_space);
}

int parse_request_length(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_trim_space(s, pos_ptr, end, item_value, digital);
}

int parse_bytes_sent(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_trim_space(s, pos_ptr, end, item_value, digital);
}

int parse_upstream_addr(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_trim_space(s, pos_ptr, end, item_value, not_space);
}

int parse_upstream_status(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_trim_space(s, pos_ptr, end, item_value, digital_or_none_end);
}

int parse_request_time(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_trim_space(s, pos_ptr, end, item_value, digital_dot);
}

int parse_upstream_response_time(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_trim_space(s, pos_ptr, end, item_value, digital_dot_minus);
}

int parse_upstream_connect_time(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_trim_space(s, pos_ptr, end, item_value, digital_dot_minus);
}

int parse_upstream_header_time(const char *s, const char **pos_ptr, const char *end, char *item_value)
{
    return parse_item_trim_space(s, pos_ptr, end, item_value, digital_dot_minus);
}

static inline void byteFormat(unsigned long s, char *out)
{
    char *unit = "KMGTPEZY";
    if (s < 1024)
    {
        sprintf(out, "%lu B", s);
        return;
    }
    unit--;
    double n = (double)s;
    while (n >= 1024)
    {
        n /= 1024;
        unit++;
    }
    sprintf(out, "%.2f %cB", n, *unit);
}

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

int main(int argc, char *argv[])
{
    if (argc > 2)
    {
        return query(argv[1], argv[2]);
    }
    FILE *input;
    if (argc < 2)
    {
        input = stdin;
    }
    else
    {
        input = fopen(argv[1], "r");
        if (input == NULL)
        {
            perror(argv[1]);
            return 1;
        }
    }

    int res = db_prepare();
    CHECK(res);
    res = db_begin();
    CHECK(res);

    char s[8192];

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

    char value[1024] = {0};

    while (fgets(s, sizeof s, input) != NULL)
    {
        const char *pos = s;
        const char *end = s + strlen(s);

        if (parse_remote_addr(s, &pos, end, remote_addr) < 0)
        {
            goto error_line;
        }
        if (parse_remote_user(s, &pos, end, remote_user) < 0)
        {
            goto error_line;
        }
        if (parse_time_local(s, &pos, end, value) < 0)
        {
            goto error_line;
        }
        long time_local = unix_time(value);
        if (parse_request_line(s, &pos, end, request_line) < 0)
        {
            goto error_line;
        }
        if (parse_status_code(s, &pos, end, value) < 0)
        {
            goto error_line;
        }
        int status_code = atoi(value);
        if (parse_body_bytes_sent(s, &pos, end, value) < 0)
        {
            goto error_line;
        }
        int body_bytes_sent = atoi(value);
        if (parse_http_referer(s, &pos, end, http_referer) < 0)
        {
            goto error_line;
        }
        if (parse_http_user_agent(s, &pos, end, http_user_agent) < 0)
        {
            goto error_line;
        }
        if (parse_http_x_forwarded_for(s, &pos, end, http_x_forwarded_for) < 0)
        {
            goto error_line;
        }
        if (parse_host(s, &pos, end, host) < 0)
        {
            goto error_line;
        }
        if (parse_request_length(s, &pos, end, value) < 0)
        {
            goto error_line;
        }
        int request_length = atoi(value);
        if (parse_bytes_sent(s, &pos, end, value) < 0)
        {
            goto error_line;
        }
        int bytes_sent = atoi(value);
        if (parse_upstream_addr(s, &pos, end, upstream_addr) < 0)
        {
            goto error_line;
        }
        if (parse_upstream_status(s, &pos, end, value) < 0)
        {
            goto error_line;
        }
        int upstream_status = atoi(value);
        if (parse_request_time(s, &pos, end, value) < 0)
        {
            goto error_line;
        }
        double request_time = atof(value);
        if (parse_upstream_response_time(s, &pos, end, value) < 0)
        {
            goto error_line;
        }
        double upstream_response_time = atof(value);
        if (parse_upstream_connect_time(s, &pos, end, value) < 0)
        {
            goto error_line;
        }
        double upstream_connect_time = atof(value);
        if (parse_upstream_header_time(s, &pos, end, value) < 0)
        {
            goto error_line;
        }
        double upstream_header_time = atof(value);

        // 这一行 所有都已正确解析，插入table中
        total_lines++;
        total_bytes_sent += bytes_sent;
        total_bytes_recv += request_length;

        res = db_insert(time_local, remote_addr, remote_user, request_line, status_code, body_bytes_sent, http_referer, http_user_agent, http_x_forwarded_for, host, request_length, bytes_sent, upstream_addr, upstream_status, request_time, upstream_response_time, upstream_connect_time, upstream_header_time);
        CHECK(res);

        continue;

    error_line:

        fprintf(stderr, "%s", s);
    }

    // 分析完毕后，排序然后，打印统计数据

    char b1[64] = {0};
    char b2[64] = {0};
    byteFormat(total_bytes_sent, b1);
    byteFormat(total_bytes_recv, b2);
    printf("共处理%d行,发送数据%s,接收数据%s\n", total_lines, b1, b2);
    return db_end();
}
