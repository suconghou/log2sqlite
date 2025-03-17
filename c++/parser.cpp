#include <string>
#include <cstring>
#include <fstream>
#include <sstream>
#include <iostream>
#include <set>

typedef bool (*char_is_match)(unsigned char x, unsigned char y);

// 仅数字
bool digital(unsigned char x, unsigned char y)
{
    return x >= 48 && x <= 57;
}

// 包含数字和.号
bool digital_dot(unsigned char x, unsigned char y)
{
    return (x >= 48 && x <= 57) || x == 46;
}

// 包含数字字母[a-f]和.号或:号（IPv4或IPv6）
bool digital_dot_colon(unsigned char x, unsigned char y)
{
    return (x >= 48 && x <= 58) || x == 46 || (x >= 97 && x <= 102);
}

// 包含数字和.号或-号
bool digital_dot_minus(unsigned char x, unsigned char y)
{
    return (x >= 48 && x <= 57) || x == 46 || x == 45;
}

// 非空格
bool not_space(unsigned char x, unsigned char y)
{
    return x != 32;
}

// 当前是空格，上一个是-或者数字
bool digital_or_none_end(unsigned char x, unsigned char y)
{
    return !(x == 32 && ((y >= 48 && y <= 57) || y == 45));
}

class Line
{
private:
    const char *ptr;       // 当前处理位置的指针
    const char *const str; // 字符串起始位置
    const char *const end; // 字符串结束位置的指针

    int parse_item_trim_space(char *item_value, const char_is_match cond)
    {
        while (ptr < end && *ptr == ' ')
        {
            ++ptr;
        }
        const char *found_start = nullptr;
        const char *found_end = nullptr;
        unsigned char y = (ptr > str) ? *(ptr - 1) : 0;
        while (ptr < end)
        {
            unsigned char x = *ptr++;
            if (cond(x, y))
            {
                found_end = ptr - 1;
                if (!found_start)
                {
                    found_start = found_end;
                }
                if (ptr < end)
                {
                    y = x;
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
            while (ptr < end && *ptr == ' ')
            {
                ++ptr;
            }
            return found_start - str;
        }
        return found_start ? (found_start - str) : -1;
    }

    int parse_item_wrap_string(char *item_value, const char &left = '"', const char &right = '"')
    {
        while (ptr < end && *ptr == ' ')
        {
            ++ptr;
        }
        if (ptr >= end || *ptr != left)
        {
            return -1;
        }
        ++ptr;
        const char *p = static_cast<const char *>(memchr(ptr, right, end - ptr));
        if (!p)
        {
            return -1;
        }
        const int v_len = p - ptr;
        memcpy(item_value, ptr, v_len);
        item_value[v_len] = '\0';
        ptr = p + 1;
        return 1;
    }

public:
    Line(char const *str);

    int parse_remote_addr(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot_colon);
    }

    int parse_remote_user(char *item_value)
    {
        while (ptr < end && *ptr == '-')
        {
            ++ptr;
        }
        return parse_item_trim_space(item_value, not_space);
    }

    int parse_time_local(char *item_value)
    {
        return parse_item_wrap_string(item_value, '[', ']');
    }

    int parse_request_line(char *item_value)
    {
        return parse_item_wrap_string(item_value);
    }

    int parse_status_code(char *item_value)
    {
        return parse_item_trim_space(item_value, digital);
    }

    int parse_body_bytes_sent(char *item_value)
    {
        return parse_item_trim_space(item_value, digital);
    }

    int parse_http_referer(char *item_value)
    {
        return parse_item_wrap_string(item_value);
    }

    int parse_http_user_agent(char *item_value)
    {
        return parse_item_wrap_string(item_value);
    }

    int parse_http_x_forwarded_for(char *item_value)
    {
        return parse_item_wrap_string(item_value);
    }

    int parse_host(char *item_value)
    {
        return parse_item_trim_space(item_value, not_space);
    }

    int parse_request_length(char *item_value)
    {
        return parse_item_trim_space(item_value, digital);
    }

    int parse_bytes_sent(char *item_value)
    {
        return parse_item_trim_space(item_value, digital);
    }

    int parse_upstream_addr(char *item_value)
    {
        return parse_item_trim_space(item_value, not_space);
    }

    int parse_upstream_status(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_or_none_end);
    }

    int parse_request_time(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot);
    }

    int parse_upstream_response_time(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot_minus);
    }

    int parse_upstream_connect_time(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot_minus);
    }

    int parse_upstream_header_time(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot_minus);
    }
};

Line::Line(const char *const line) : ptr(line), str(line), end(line + strlen(line))
{
}