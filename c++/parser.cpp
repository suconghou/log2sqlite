#include <string>
#include <cstring>
#include <fstream>
#include <sstream>
#include <iostream>
#include <set>

using namespace std;

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

class Line
{
private:
    int index;
    int len;
    char const *str;

    int
    parse_item_trim_space(char *item_value, char_is_match cond, int strip_square, int result_strip_left_quote)
    {
        int pad = 1;
        int found_start = -1;
        int found_end = -1;
        int gotvalue = -1;
        int i = index;
        while (i < len)
        {
            unsigned char x = str[i];
            i++;
            if ((x == ' ' || (strip_square && x == '[')) && pad)
            {
                index = i;
                continue;
            }
            else
            {
                pad = 0;
            }
            if (gotvalue < 0)
            {
                unsigned char y = i < len ? str[i] : 0;
                unsigned char z = i >= 2 ? str[i - 2] : 0;
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
                    goto end;
                }
                // 包含cond成立时当前字符x,如果结果字符串以某字符开始，我们配置了result_strip_left_quote开头
                // 例如解析request_line,开头有引号,我们仅在生成结果时过滤
                if (result_strip_left_quote > 0)
                {
                    while (found_start < found_end)
                    {
                        if (str[found_start] == 34)
                        {
                            found_start++;
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                int v_len = found_end - found_start + 1;
                strncpy(item_value, str + found_start, v_len);
                item_value[v_len] = '\0';
                gotvalue = 1;
                if (i >= len)
                {
                    if (found_end == len - 1 || x == ' ')
                    {
                        // 字符串已完全遍历
                        index = len;
                    }
                    else
                    {
                        index = found_end + 1;
                    }
                }
            }
            else
            {
                if (x == ' ' || (strip_square && x == ']'))
                {
                    if (i < len)
                    {
                        continue;
                    }
                    else if (i == len)
                    {
                        i++;
                    }
                }
                index = i - 1;
                break;
            }
        }
    end:
        return found_start;
    }

public:
    Line(char const *str);

    int parse_remote_addr(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot, 0, 0);
    }

    int parse_remote_user(char *item_value)
    {
        int i = index;
        while (i < len)
        {
            if (str[i] == '-')
            {
                i++;
            }
            else
            {
                break;
            }
        }
        index = i;
        return parse_item_trim_space(item_value, not_space, 0, 0);
    }

    int parse_time_local(char *item_value)
    {
        return parse_item_trim_space(item_value, square_right_space, 1, 0);
    }

    int parse_request_line(char *item_value)
    {
        space_count = 0;
        return parse_item_trim_space(item_value, request_line_quote_end, 0, 1);
    }

    int parse_status_code(char *item_value)
    {
        return parse_item_trim_space(item_value, digital, 0, 0);
    }

    int parse_body_bytes_sent(char *item_value)
    {
        return parse_item_trim_space(item_value, digital, 0, 0);
    }

    int parse_http_referer(char *item_value)
    {
        quote_count = 0;
        return parse_item_trim_space(item_value, quote_string_end, 0, 1);
    }

    int parse_http_user_agent(char *item_value)
    {
        quote_count = 0;
        return parse_item_trim_space(item_value, quote_string_end, 0, 1);
    }

    int parse_http_x_forwarded_for(char *item_value)
    {
        quote_count = 0;
        return parse_item_trim_space(item_value, quote_string_end, 0, 1);
    }

    int parse_host(char *item_value)
    {
        return parse_item_trim_space(item_value, string_end, 0, 0);
    }

    int parse_request_length(char *item_value)
    {
        return parse_item_trim_space(item_value, digital, 0, 0);
    }

    int parse_bytes_sent(char *item_value)
    {
        return parse_item_trim_space(item_value, digital, 0, 0);
    }

    int parse_upstream_addr(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_or_none_end, 0, 0);
    }

    int parse_upstream_status(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_or_none_end, 0, 0);
    }

    int parse_request_time(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot, 0, 0);
    }

    int parse_upstream_response_time(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot_minus, 0, 0);
    }

    int parse_upstream_connect_time(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot_minus, 0, 0);
    }

    int parse_upstream_header_time(char *item_value)
    {
        return parse_item_trim_space(item_value, digital_dot_minus, 0, 0);
    }
};

Line::Line(char const *line)
{
    str = line;
    index = 0;
    len = strlen(line);
}
