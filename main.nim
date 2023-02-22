

import os, tables, sets, strformat, strutils, sqlite3, db_sqlite, times


type Line = object
    index: int
    str: string

const blank = {' '}
const quotation = {'"'}
const square_left = {' ', '['}
const square_right = {' ', ']'}

# 根据条件解析，并去除前置后置x字符
proc parse_item_trimx(this: var Line, left: set[char], right: set[char], cond: proc, strip_left: set[char] = {}): string =
    let strlen = this.str.len;
    var i = this.index;
    var item_value: string;
    var pad = true;
    var found_start = -1;
    var found_end = -1;
    while i < strlen:
        let x = this.str[i]
        i+=1; # i已指向下一个字符
        if x in left and pad:
            # 去前置连续x
            this.index = i;
            continue
        else:
            pad = false
        if item_value.len < 1:
            let y = if i < strlen: this.str[i] else: '\x00'
            let z = if i >= 2: this.str[i-2] else: '\x00'
            # 符合预定格式,x为当前字符,y为下个字符,y可能为0,z为上一个字符
            if cond(x, y, z):
                found_end = i-1
                if found_start < 0:
                    found_start = found_end
                # 如果未到行末尾,才能continue,否则i=len了,不能continue,会造成下次退出循环,item_value未赋值
                if i < strlen:
                    continue
            # 否则匹配到边界或者完全没有匹配到
            if found_start < 0:
                # 完全没有匹配到
                raise newException(ValueError, "匹配失败:"&this.str)
            # 包含cond成立时当前字符x,如果结果字符串以某字符开始，我们配置了strip_left开头
            # 例如解析request_line,开头有引号,我们仅在生成结果时过滤
            if strip_left.len > 0:
                while found_start < found_end:
                    if this.str[found_start] in strip_left:
                        found_start+=1
                    else:
                        break;
            item_value = this.str.substr(found_start, found_end)
            # 执行到此处，已匹配到了想要的字符串，要么匹配到字符串结尾了，要么中途中断，要么匹配到最后一个字符了但不符合
            if i >= strlen:
                # 如果中途中断，则不会进入此条件，下次循环会进去后置特定字符逻辑
                # 如果最后一个字符符合，则found=len-1,此时剩余应为空
                # 如果最后一个字符不符合，判断是否是后置字符，如果是则也为空
                if found_end == strlen-1 or x in right:
                    this.index = strlen-1
                else:
                    this.index = found_end+1
        else:
            if x in right:
                # 去后置特定字符,如果当前字符就是最后一个字符，并且当前字符是特定字符，则i需等于len,以完全裁剪
                if i < strlen:
                    continue;
                elif i == strlen:
                    i+=1
            this.index = i-1
            break
    # 防止前置字符去除时，直接continue完所有
    if item_value.len < 1:
        raise newException(ValueError, "匹配失败:"&this.str)
    return item_value

# 仅数字
proc digital(x: char, y: char, z: char): bool = x >= '\48' and x <= '\57'

# 包含数字和.号
proc digital_dot(x: char, y: char, z: char): bool = (x >= '\48' and x <= '\57') or x == '\46'

# 包含数字和.号或-号
proc digital_dot_minus(x: char, y: char, z: char): bool = (x >= '\48' and x <= '\57') or x == '\46' or x == '\45'

# 匹配到],并且下一个是空格
proc square_right_space(x: char, y: char, z: char): bool = not (x == '\93' and y == '\32')

# 非空格
proc not_space(x: char, y: char, z: char): bool = x != '\32'

# 包含2个双引号，匹配到第二个双引号结束
proc quote_string_end(): proc =
    var q = 0;
    return proc (x: char, y: char, z: char): bool =
        if x == '\34':
            q+=1
        if (x == '\34' and q == 2):
            return false
        return true

# 当前字符是空格，上个字符是字母,不包含空格
proc string_end(x: char, y: char, z: char): bool = not (x == '\32' and ( (z >= '\65' and z <= '\90') or (z >= '\97' and z <= '\122')))

# 当前是空格，上一个是-或者数字
proc digital_or_none_end(x: char, y: char, z: char): bool = not (x == '\32' and ( (z >= '\48' and z <= '\57') or z == '\45'))

# 包含数字和.号
proc parse_remote_addr(this: var Line): string =
    return this.parse_item_trimx(blank, blank, digital_dot)

# 去除可能存在的-,非空格
proc parse_remote_user(this: var Line): string =
    let strlen = this.str.len
    while this.index < strlen:
        if this.str[this.index] == '\45':
            this.index+=1
        else:
            break;
    return this.parse_item_trimx(blank, blank, not_space)

# 匹配到],并且下一个是空格
proc parse_time_local(this: var Line): string =
    return this.parse_item_trimx(square_left, square_right, square_right_space)

# 当前字符是双引号，下个字符是空格，上个字符是http版本,并且只能包含2个空格
proc parse_request_line(this: var Line): string =
    var c = 0;
    proc v(x: char, y: char, z: char): bool =
        if x == '\32':
            c+=1
        if c > 2:
            return false
        return not (x == '\34' and y == '\32' and (z == '\49' or z == '\48'))
    return this.parse_item_trimx(blank, blank, v, quotation)

# 是数字
proc parse_status_code(this: var Line): string =
    return this.parse_item_trimx(blank, blank, digital)

# 是数字
proc parse_body_bytes_sent(this: var Line): string =
    return this.parse_item_trimx(blank, blank, digital)

# 当前字符是双引号，下个字符是空格
proc parse_http_referer(this: var Line): string =
    return this.parse_item_trimx(blank, blank, quote_string_end(), quotation)

# 当前字符是双引号，下个字符是空格
proc parse_http_user_agent(this: var Line): string =
    return this.parse_item_trimx(blank, blank, quote_string_end(), quotation)

# 当前字符是双引号，下个字符是空格
proc parse_http_x_forwarded_for(this: var Line): string =
    return this.parse_item_trimx(blank, blank, quote_string_end(), quotation)

# 当前字符是空格，上个字符是字母
proc parse_host(this: var Line): string =
    return this.parse_item_trimx(blank, blank, string_end)

# 数字
proc parse_request_length(this: var Line): string =
    return this.parse_item_trimx(blank, blank, digital)

# 数字
proc parse_bytes_sent(this: var Line): string =
    return this.parse_item_trimx(blank, blank, digital)

# 当前是空格，上一个是-或者数字
proc parse_upstream_addr(this: var Line): string =
    return this.parse_item_trimx(blank, blank, digital_or_none_end)

# 当前是空格，上一个是-或者数字
proc parse_upstream_status(this: var Line): string =
    return this.parse_item_trimx(blank, blank, digital_or_none_end)

# 数字和.号
proc parse_request_time(this: var Line): string =
    return this.parse_item_trimx(blank, blank, digital_dot)

# 数字和.号，或者-
proc parse_upstream_response_time(this: var Line): string =
    return this.parse_item_trimx(blank, blank, digital_dot_minus)

# 数字和.号，或者-
proc parse_upstream_connect_time(this: var Line): string =
    return this.parse_item_trimx(blank, blank, digital_dot_minus)

# 数字和.号，或者-
proc parse_upstream_header_time(this: var Line): string =
    return this.parse_item_trimx(blank, blank, digital_dot_minus)


template f(str: string): float =
    if str == "-": 0.float else: parseFloat(str)

template i(str: string): int =
    try:
        if str == "-": 0 else: parseInt(str)
    except:
        parseInt(str.split(',')[0])

proc process(filename: File|string) =

    var total_bytes_sent: int64 = 0;
    var total_bytes_recv: int64 = 0;
    var total_lines: uint = 0;

    let db = open("nginx_log.db", "", "", "")

    db.exec(sql"PRAGMA JOURNAL_MODE=MEMORY")
    db.exec(sql"PRAGMA LOCKING_MODE=EXCLUSIVE")
    db.exec(sql"PRAGMA SYNCHRONOUS=OFF")
    db.exec(sql"PRAGMA CACHE_SIZE=8192")
    db.exec(sql"PRAGMA PAGE_SIZE=4096")
    db.exec(sql"PRAGMA TEMP_STORE=MEMORY")
    db.exec(sql"DROP TABLE IF EXISTS 'nginx_log'")
    db.exec(sql"CREATE TABLE 'nginx_log' ( 'id' integer NOT NULL PRIMARY KEY AUTOINCREMENT, 'time' integer NOT NULL, 'remote_addr' text NOT NULL, 'remote_user' text NOT NULL, 'request' text NOT NULL, 'status' integer NOT NULL, 'body_bytes_sent' integer NOT NULL, 'http_referer' text NOT NULL, 'http_user_agent' text NOT NULL, 'http_x_forwarded_for' text NOT NULL, 'host' text NOT NULL, 'request_length' integer NOT NULL, 'bytes_sent' integer NOT NULL, 'upstream_addr' text NOT NULL, 'upstream_status' integer NOT NULL, 'request_time' real NOT NULL, 'upstream_response_time' real NOT NULL, 'upstream_connect_time' real NOT NULL, 'upstream_header_time' real NOT NULL )")
    db.exec(sql"CREATE INDEX 'log_time' ON 'nginx_log' ('time')")


    db.exec(sql"BEGIN;")
    let insertStmt = db.prepare("INSERT INTO `nginx_log`(time,remote_addr,remote_user,request,status,body_bytes_sent,http_referer,http_user_agent,http_x_forwarded_for,host,request_length,bytes_sent,upstream_addr,upstream_status,request_time,upstream_response_time,upstream_connect_time,upstream_header_time) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);")



    proc parse_line(line: string) =
        var l = Line(str: line)
        let remote_addr = l.parse_remote_addr()
        let remote_user = l.parse_remote_user()
        let time_local = l.parse_time_local()
        let request_line = l.parse_request_line()
        let status_code = l.parse_status_code()
        let body_bytes_sent = l.parse_body_bytes_sent()
        let http_referer = l.parse_http_referer()
        let http_user_agent = l.parse_http_user_agent()
        let http_x_forwarded_for = l.parse_http_x_forwarded_for()
        let host = l.parse_host()
        let request_length = l.parse_request_length()
        let bytes_sent = l.parse_bytes_sent()
        let upstream_addr = l.parse_upstream_addr()
        let upstream_status = l.parse_upstream_status()
        let request_time = l.parse_request_time()
        let upstream_response_time = l.parse_upstream_response_time()
        let upstream_connect_time = l.parse_upstream_connect_time()
        let upstream_header_time = l.parse_upstream_header_time()


        let sent_num = parseInt(bytes_sent)
        let recv_num = parseInt(request_length)

        let time = parse(time_local, "d/MMM/YYYY:hh:mm:ss ZZZ").toTime().toUnix()
        db.exec(insertStmt, time, remote_addr, remote_user, request_line, parseInt(status_code), parseInt(body_bytes_sent), http_referer, http_user_agent, http_x_forwarded_for, host, recv_num, sent_num, upstream_addr, upstream_status.i, parseFloat(request_time), upstream_response_time.f, upstream_connect_time.f, upstream_header_time.f)

        total_bytes_sent += sent_num
        total_bytes_recv += recv_num




    for line in filename.lines:
        try:
            parse_line(line);
            total_lines += 1;
        except:
            stderr.writeLine(line)
    # 分析完毕

    db.exec(sql"COMMIT;")

    echo &"共处理{total_lines}行,发送数据{formatSize(total_bytes_sent)},接收数据{formatSize(total_bytes_recv)}\n"




try:
    if paramCount() > 0:
        process(paramStr(1))
    else:
        process(stdin)
except:
    echo getCurrentExceptionMsg()
    quit(1)


