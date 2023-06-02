#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include "db.c"

typedef int (*char_is_match)(unsigned char x, unsigned char y, unsigned char z);

// 此变量解析每行时需置为0，模拟闭包
int space_count = 0;
// 当前字符是双引号，下个字符是空格，上个字符是http版本,并且只能包含2个空格
int request_line_quote_end(unsigned char x, unsigned char y, unsigned char z)
{
    if (x == 32)
    {
        space_count++;
    }
    if (space_count > 2)
    {
        return 0;
    }
    if (x == 34 && y == 32 && (z == 49 || z == 48))
    {
        return 0;
    }
    return 1;
}

// 仅数字
int digital(unsigned char x, unsigned char y, unsigned char z)
{
    return x >= 48 && x <= 57;
}

// 包含数字和.号
int digital_dot(unsigned char x, unsigned char y, unsigned char z)
{
    return (x >= 48 && x <= 57) || x == 46;
}

// 包含数字字母和.号或:号（IPv4或IPv6）
int digital_dot_colon(unsigned char x, unsigned char y, unsigned char z)
{
    return (x >= 48 && x <= 58) || x == 46 || (x >= 97 && x <= 122);
}

// 包含数字和.号或-号
int digital_dot_minus(unsigned char x, unsigned char y, unsigned char z)
{
    return (x >= 48 && x <= 57) || x == 46 || x == 45;
}

// 匹配到],并且下一个是空格
int square_right_space(unsigned char x, unsigned char y, unsigned char z)
{
    if (x == 93 && y == 32)
    {
        return 0;
    }
    return 1;
}

// 非空格
int not_space(unsigned char x, unsigned char y, unsigned char z)
{
    return x != 32;
}

// 包含2个双引号，匹配到第二个双引号结束
int quote_count = 0;
int quote_string_end(unsigned char x, unsigned char y, unsigned char z)
{
    if (x == 34)
    {
        quote_count++;
    }
    if (x == 34 && quote_count == 2)
    {
        return 0;
    }
    return 1;
}

// 当前字符是空格，上个字符是字母,不包含空格
int string_end(unsigned char x, unsigned char y, unsigned char z)
{
    if (x == 32 && ((z >= 65 && z <= 90) || (z >= 97 && z <= 122)))
    {
        return 0;
    }
    return 1;
}

// 当前是空格，上一个是-或者数字
int digital_or_none_end(unsigned char x, unsigned char y, unsigned char z)
{
    if (x == 32 && ((z >= 48 && z <= 57) || z == 45))
    {
        return 0;
    }
    return 1;
}

// offset 字符串坐标
int parse_item_trim_space(const char *s, int *offset, int len, char *item_value, char_is_match cond, int strip_square, int result_strip_left_quote)
{
    int i = *offset;
    while (i < len)
    {
        unsigned char x = s[i];
        if (x == ' ' || (strip_square && x == '['))
        {
            i++;
        }
        else
        {
            break;
        }
    }
    *offset = i;
    int found_start = -1;
    int found_end = -1;
    while (i < len)
    {
        const unsigned char x = s[i];
        i++;
        const unsigned char y = i < len ? s[i] : 0;
        const unsigned char z = i >= 2 ? s[i - 2] : 0;
        if (cond(x, y, z))
        {
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
        // 包含cond成立时当前字符x,如果结果字符串以某字符开始，我们配置了result_strip_left_quote开头
        // 例如解析request_line,开头有引号,我们仅在生成结果时过滤
        if (result_strip_left_quote > 0)
        {
            while (found_start < found_end)
            {
                if (s[found_start] == 34)
                {
                    found_start++;
                }
                else
                {
                    break;
                }
            }
        }
        const int v_len = found_end - found_start + 1;
        strncpy(item_value, s + found_start, v_len);
        item_value[v_len] = '\0';
        if (i >= len)
        {
            if (found_end == len - 1 || x == ' ')
            {
                // 字符串已完全遍历
                *offset = len;
            }
            else
            {
                *offset = found_end + 1;
            }
        }
        else
        {
            while (i < len)
            {
                const unsigned char x = s[i];
                if (x == ' ' || (strip_square && x == ']'))
                {
                    i++;
                }
                else
                {
                    break;
                }
            }
            *offset = i;
        }
        return found_start;
    }
    return found_start;
}

int parse_remote_addr(const char *s, int *offset, int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital_dot_colon, 0, 0);
}

int parse_remote_user(const char *s, int *offset, int len, char *item_value)
{
    int i = *offset;
    while (i < len)
    {
        if (s[i] == '-')
        {
            i++;
        }
        else
        {
            break;
        }
    }
    *offset = i;
    return parse_item_trim_space(s, offset, len, item_value, not_space, 0, 0);
}

int parse_time_local(const char *s, int *offset, int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, square_right_space, 1, 0);
}

int parse_request_line(const char *s, int *offset, int len, char *item_value)
{
    space_count = 0;
    return parse_item_trim_space(s, offset, len, item_value, request_line_quote_end, 0, 1);
}

int parse_status_code(const char *s, int *offset, int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital, 0, 0);
}

int parse_body_bytes_sent(const char *s, int *offset, int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital, 0, 0);
}

int parse_http_referer(const char *s, int *offset, int len, char *item_value)
{
    quote_count = 0;
    return parse_item_trim_space(s, offset, len, item_value, quote_string_end, 0, 1);
}

int parse_http_user_agent(const char *s, int *offset, int len, char *item_value)
{
    quote_count = 0;
    return parse_item_trim_space(s, offset, len, item_value, quote_string_end, 0, 1);
}

int parse_http_x_forwarded_for(const char *s, int *offset, int len, char *item_value)
{
    quote_count = 0;
    return parse_item_trim_space(s, offset, len, item_value, quote_string_end, 0, 1);
}

int parse_host(const char *s, int *offset, int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, string_end, 0, 0);
}

int parse_request_length(const char *s, int *offset, int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital, 0, 0);
}

int parse_bytes_sent(const char *s, int *offset, int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital, 0, 0);
}

int parse_upstream_addr(const char *s, int *offset, int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital_or_none_end, 0, 0);
}

int parse_upstream_status(const char *s, int *offset, int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital_or_none_end, 0, 0);
}

int parse_request_time(const char *s, int *offset, int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital_dot, 0, 0);
}

int parse_upstream_response_time(const char *s, int *offset, int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital_dot_minus, 0, 0);
}

int parse_upstream_connect_time(const char *s, int *offset, int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital_dot_minus, 0, 0);
}

int parse_upstream_header_time(const char *s, int *offset, int len, char *item_value)
{
    return parse_item_trim_space(s, offset, len, item_value, digital_dot_minus, 0, 0);
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

long unix_time(const char *timestr)
{
    struct tm t = {};
    strptime(timestr, "%d/%b/%Y:%H:%M:%S", &t);
    return mktime(&t);
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
    char value[8192] = {0};

    while (fgets(s, sizeof s, input) != NULL)
    {
        int offset = 0;
        int len = strlen(s);

        char *remote_addr = NULL;
        char *remote_user = NULL;
        long time_local = 0;
        char *request_line = NULL;
        int status_code = 0;
        char *http_referer = NULL;
        char *http_user_agent = NULL;
        char *http_x_forwarded_for = NULL;
        char *host = NULL;
        int request_length = 0;
        int bytes_sent = 0;
        char *upstream_addr = NULL;
        int upstream_status = 0;
        double request_time = 0;
        double upstream_response_time = 0;
        double upstream_connect_time = 0;
        double upstream_header_time = 0;

        if (parse_remote_addr(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        remote_addr = malloc(strlen(value) + 1);
        strcpy(remote_addr, value);
        if (parse_remote_user(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        remote_user = malloc(strlen(value) + 1);
        strcpy(remote_user, value);
        if (parse_time_local(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        time_local = unix_time(value);
        if (parse_request_line(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        request_line = malloc(strlen(value) + 1);
        strcpy(request_line, value);
        if (parse_status_code(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        status_code = atoi(value);
        if (parse_body_bytes_sent(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        int body_bytes_sent = atoi(value);
        if (parse_http_referer(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        http_referer = malloc(strlen(value) + 1);
        strcpy(http_referer, value);
        if (parse_http_user_agent(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        http_user_agent = malloc(strlen(value) + 1);
        strcpy(http_user_agent, value);
        if (parse_http_x_forwarded_for(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        http_x_forwarded_for = malloc(strlen(value) + 1);
        strcpy(http_x_forwarded_for, value);
        if (parse_host(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        host = malloc(strlen(value) + 1);
        strcpy(host, value);
        if (parse_request_length(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        request_length = atoi(value);
        if (parse_bytes_sent(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        bytes_sent = atoi(value);
        if (parse_upstream_addr(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        upstream_addr = malloc(strlen(value) + 1);
        strcpy(upstream_addr, value);
        if (parse_upstream_status(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        upstream_status = atoi(value);
        if (parse_request_time(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        request_time = atof(value);
        if (parse_upstream_response_time(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        upstream_response_time = atof(value);
        if (parse_upstream_connect_time(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        upstream_connect_time = atof(value);
        if (parse_upstream_header_time(s, &offset, len, value) < 0)
        {
            goto error_line;
        }
        upstream_header_time = atof(value);

        // 这一行 所有都已正确解析，插入table中
        total_lines++;
        total_bytes_sent += bytes_sent;
        total_bytes_recv += request_length;

        res = db_insert(time_local, remote_addr, remote_user, request_line, status_code, body_bytes_sent, http_referer, http_user_agent, http_x_forwarded_for, host, request_length, bytes_sent, upstream_addr, upstream_status, request_time, upstream_response_time, upstream_connect_time, upstream_header_time);
        CHECK(res);

        free(remote_addr);
        free(remote_user);
        free(request_line);
        free(http_referer);
        free(http_user_agent);
        free(http_x_forwarded_for);
        free(host);
        free(upstream_addr);
        continue;

    error_line:
        free(remote_addr);
        free(remote_user);
        free(request_line);
        free(http_referer);
        free(http_user_agent);
        free(http_x_forwarded_for);
        free(host);
        free(upstream_addr);
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
