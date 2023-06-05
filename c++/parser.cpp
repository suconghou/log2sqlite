#include <string>
#include <cstring>
#include <fstream>
#include <sstream>
#include <iostream>
#include <set>

using namespace std;

typedef bool (*char_is_match)(unsigned char x, unsigned char y, unsigned char z);

// 仅数字
bool digital(unsigned char x, unsigned char y, unsigned char z)
{
    return x >= 48 && x <= 57;
}

// 包含数字和.号
bool digital_dot(unsigned char x, unsigned char y, unsigned char z)
{
    return (x >= 48 && x <= 57) || x == 46;
}

// 包含数字字母和.号或:号（IPv4或IPv6）
bool digital_dot_colon(unsigned char x, unsigned char y, unsigned char z)
{
    return (x >= 48 && x <= 58) || x == 46 || (x >= 97 && x <= 122);
}

// 包含数字和.号或-号
bool digital_dot_minus(unsigned char x, unsigned char y, unsigned char z)
{
    return (x >= 48 && x <= 57) || x == 46 || x == 45;
}

// 匹配到],并且下一个是空格
bool square_right_space(unsigned char x, unsigned char y, unsigned char z)
{
    return !(x == 93 && y == 32);
}

// 非空格
bool not_space(unsigned char x, unsigned char y, unsigned char z)
{
    return x != 32;
}

// 当前字符是空格，上个字符是字母,不包含空格
bool string_end(unsigned char x, unsigned char y, unsigned char z)
{
    return !(x == 32 && ((z >= 65 && z <= 90) || (z >= 97 && z <= 122)));
}

// 当前是空格，上一个是-或者数字
bool digital_or_none_end(unsigned char x, unsigned char y, unsigned char z)
{
    return !(x == 32 && ((z >= 48 && z <= 57) || z == 45));
}

class Line
{
private:
    int index;
    int len;
    char const *str;

    int
    parse_item_trim_space(char *item_value, char_is_match cond, bool strip_square)
    {
        unsigned char x, y, z;
        while (index < len)
        {
            x = str[index];
            if (x == ' ' || (strip_square && x == '['))
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
            y = index < len ? str[index] : 0;
            z = index >= 2 ? str[index - 2] : 0;
            if (cond(x, y, z))
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
                if (x == ' ' || (strip_square && x == ']'))
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

    int parse_item_quote_string(char *item_value)
    {
        int quote_start = -1;
        while (index < len)
        {
            if (quote_start < 0)
            {
                if (str[index] == ' ')
                {
                    index++;
                    continue;
                }
                else if (str[index] == '"')
                {
                    quote_start = index;
                    index++;
                    continue;
                }
                else
                {
                    return -1;
                }
            }
            if (str[index] == '"')
            {
                const int v_len = index - quote_start - 1;
                // 不包含quote_start的位置，也不包含最后index的位置
                memcpy(item_value, str + quote_start + 1, v_len);
                item_value[v_len] = '\0';
                index++;
                return quote_start;
            }
            else
            {
                index++;
            }
        }
        return -1;
    }

public:
    Line(char const *str);

    int parse_remote_addr(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot_colon, false);
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
        return parse_item_trim_space(item_value, not_space, false);
    }

    int parse_time_local(char *item_value)
    {
        return parse_item_trim_space(item_value, square_right_space, true);
    }

    int parse_request_line(char *item_value)
    {
        return parse_item_quote_string(item_value);
    }

    int parse_status_code(char *item_value)
    {
        return parse_item_trim_space(item_value, digital, false);
    }

    int parse_body_bytes_sent(char *item_value)
    {
        return parse_item_trim_space(item_value, digital, false);
    }

    int parse_http_referer(char *item_value)
    {
        return parse_item_quote_string(item_value);
    }

    int parse_http_user_agent(char *item_value)
    {
        return parse_item_quote_string(item_value);
    }

    int parse_http_x_forwarded_for(char *item_value)
    {
        return parse_item_quote_string(item_value);
    }

    int parse_host(char *item_value)
    {
        return parse_item_trim_space(item_value, string_end, false);
    }

    int parse_request_length(char *item_value)
    {
        return parse_item_trim_space(item_value, digital, false);
    }

    int parse_bytes_sent(char *item_value)
    {
        return parse_item_trim_space(item_value, digital, false);
    }

    int parse_upstream_addr(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_or_none_end, false);
    }

    int parse_upstream_status(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_or_none_end, false);
    }

    int parse_request_time(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot, false);
    }

    int parse_upstream_response_time(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot_minus, false);
    }

    int parse_upstream_connect_time(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot_minus, false);
    }

    int parse_upstream_header_time(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot_minus, false);
    }
};

Line::Line(char const *line)
{
    str = line;
    index = 0;
    len = strlen(line);
}
