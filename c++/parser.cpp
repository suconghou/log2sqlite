#include <string>
#include <cstring>
#include <fstream>
#include <sstream>
#include <iostream>
#include <set>

using namespace std;

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

// 包含数字字母和.号或:号（IPv4或IPv6）
bool digital_dot_colon(unsigned char x, unsigned char y)
{
    return (x >= 48 && x <= 58) || x == 46 || (x >= 97 && x <= 122);
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
    int index;
    const int len;
    const char *const str;

    int
    parse_item_trim_space(char *item_value, char_is_match cond)
    {
        unsigned char x, y;
        while (index < len)
        {
            x = str[index];
            if (x == ' ')
            {
                index++;
            }
            else
            {
                break;
            }
        }
        int found_start = -1;
        int found_end = -1;
        while (index < len)
        {
            x = str[index];
            index++;
            y = index >= 2 ? str[index - 2] : 0;
            if (cond(x, y))
            {
                found_end = index - 1;
                if (found_start < 0)
                {
                    found_start = found_end;
                }
                if (index < len)
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
            memcpy(item_value, str + found_start, v_len);
            item_value[v_len] = '\0';
            while (index < len)
            {
                x = str[index];
                if (x == ' ')
                {
                    index++;
                }
                else
                {
                    break;
                }
            }
            return found_start;
        }
        return found_start;
    }

    int parse_item_wrap_string(char *item_value, const char left = '"', const char right = '"')
    {
        while (index < len)
        {
            if (str[index] == ' ')
            {
                index++;
                continue;
            }
            else if (str[index] == left)
            {
                index++;
                auto p = memchr(str + index, right, len - index);
                if (!p)
                {
                    return -1;
                }
                const int v_len = (char *)p - str - index;
                memcpy(item_value, str + index, v_len);
                item_value[v_len] = '\0';
                index += v_len + 1;
                return 1;
            }
            else
            {
                return -1;
            }
        }
        return -1;
    }

public:
    Line(char const *str);

    int parse_remote_addr(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot_colon);
    }

    int parse_remote_user(char *item_value)
    {
        while (index < len)
        {
            if (str[index] == '-')
            {
                index++;
            }
            else
            {
                break;
            }
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
        return parse_item_trim_space(item_value, digital_or_none_end);
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

Line::Line(const char *const line) : len(strlen(line)), str(line)
{
    index = 0;
}
