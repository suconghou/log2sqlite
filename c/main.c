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

// offset 字符串坐标
int parse_item_trim_space(const char *s, int *offset, const int len, char *item_value, char_is_match cond)
{
    int i = *offset;
    while (i < len && s[i] == ' ')
    {
        ++i;
    }
    *offset = i;
    int found_start = -1;
    int found_end = -1;
    unsigned char y = i > 0 ? s[i - 1] : 0;
    while (i < len)
    {
        unsigned char x = s[i++];
        if (cond(x, y))
        {
            y = x;
            found_end = i - 1;
            if (found_start < 0)
            {
                found_start = found_end;
            }
            if (i < len)
            {
                continue;
            }
        }
        if (found_start < 0)
        {
            return -1;
        }
        // cond成立时,则包含当前字符x，否则不包含，截取的字符最少1字节
        const int v_len = found_end - found_start + 1;
        memcpy(item_value, s + found_start, v_len);
        item_value[v_len] = '\0';
        while (i < len && s[i] == ' ')
        {
            ++i;
        }
        *offset = i;
        return found_start;
    }
    return found_start;
}

int parse_item_wrap_string(const char *s, int *offset, const int len, char *item_value, const char left, const char right)
{
    int i = *offset;
    while (i < len && s[i] == ' ')
    {
        ++i;
    }
    if (i >= len || s[i] != left)
    {
        return -1;
    }
    ++i;
    char *p = memchr(s + i, right, len - i);
    if (!p)
    {
        return -1;
    }
    const int v_len = p - s - i;
    memcpy(item_value, s + i, v_len);
    item_value[v_len] = '\0';
    *offset = i + v_len + 1;
    return 1;
}

int parse_remote_addr(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital_dot_colon);
}

int parse_remote_user(const char *s, int *offset, const int len, char *item_value)
{
    int i = *offset;
    while (i < len && s[i] == '-')
    {
        ++i;
    }
    *offset = i;
    return parse_item_trim_space(s, offset, len, item_value, not_space);
}

int parse_time_local(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_wrap_string(s, offset, len, item_value, '[', ']');
}

int parse_request_line(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_wrap_string(s, offset, len, item_value, '"', '"');
}

int parse_status_code(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital);
}

int parse_body_bytes_sent(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital);
}

int parse_http_referer(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_wrap_string(s, offset, len, item_value, '"', '"');
}

int parse_http_user_agent(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_wrap_string(s, offset, len, item_value, '"', '"');
}

int parse_http_x_forwarded_for(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_wrap_string(s, offset, len, item_value, '"', '"');
}

int parse_host(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, not_space);
}

int parse_request_length(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital);
}

int parse_bytes_sent(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital);
}

int parse_upstream_addr(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital_or_none_end);
}

int parse_upstream_status(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital_or_none_end);
}

int parse_request_time(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital_dot);
}

int parse_upstream_response_time(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital_dot_minus);
}

int parse_upstream_connect_time(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital_dot_minus);
}

int parse_upstream_header_time(const char *s, int *offset, const int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital_dot_minus);
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
        int offset = 0;
        int len = strlen(s);

        if (parse_remote_addr(s, &offset, len, remote_addr) < 0)
        {
            goto error_line;
        }
        if (parse_remote_user(s, &offset, len, remote_user) < 0)
        {
            goto error_line;
        }
        if (parse_time_local(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        long time_local = unix_time(value);
        if (parse_request_line(s, &offset, len, request_line) < 0)
        {
            goto error_line;
        }
        if (parse_status_code(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        int status_code = atoi(value);
        if (parse_body_bytes_sent(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        int body_bytes_sent = atoi(value);
        if (parse_http_referer(s, &offset, len, http_referer) < 0)
        {
            goto error_line;
        }
        if (parse_http_user_agent(s, &offset, len, http_user_agent) < 0)
        {
            goto error_line;
        }
        if (parse_http_x_forwarded_for(s, &offset, len, http_x_forwarded_for) < 0)
        {
            goto error_line;
        }
        if (parse_host(s, &offset, len, host) < 0)
        {
            goto error_line;
        }
        if (parse_request_length(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        int request_length = atoi(value);
        if (parse_bytes_sent(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        int bytes_sent = atoi(value);
        if (parse_upstream_addr(s, &offset, len, upstream_addr) < 0)
        {
            goto error_line;
        }
        if (parse_upstream_status(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        int upstream_status = atoi(value);
        if (parse_request_time(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        double request_time = atof(value);
        if (parse_upstream_response_time(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        double upstream_response_time = atof(value);
        if (parse_upstream_connect_time(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        double upstream_connect_time = atof(value);
        if (parse_upstream_header_time(s, &offset, len, value) < 0)
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
