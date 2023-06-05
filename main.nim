

import os, tables, sets, strformat, strutils, sqlite3, db_sqlite, times


type Line = object
    index: int
    str: string

const blank = {' '}
const square_left = {' ', '['}
const square_right = {' ', ']'}

# 类似内置substr的更快实现
func substr(s: string, first: int, last: int): string =
    let l = last-first+1
    if l < 1:
        return ""
    result = newString(l)
    copyMem(result[0].addr, cast[cstring](cast[uint](s.cstring)+first.uint), l)

# 根据条件解析，并去除前置后置x字符
proc parse_item_trimx(this: var Line, left: set[char], right: set[char], cond: proc): string =
    while this.index < this.str.len:
        if this.str[this.index] in left:
            this.index+=1
        else:
            break
    var i = this.index;
    var found_start = -1;
    var found_end = -1;
    while i < this.str.len:
        let x = this.str[i]
        i+=1; # i已指向下一个字符
        let y = if i < this.str.len: this.str[i] else: '\x00'
        let z = if i >= 2: this.str[i-2] else: '\x00'
        # 符合预定格式,x为当前字符,y为下个字符,y可能为0,z为上一个字符
        if cond(x, y, z):
            found_end = i-1
            if found_start < 0:
                found_start = found_end
            # 如果未到行末尾,才能continue,否则i=len了,不能continue,会造成下次退出循环,item_value未赋值
            if i < this.str.len:
                continue
        # 否则匹配到边界或者完全没有匹配到
        if found_start < 0:
            # 完全没有匹配到
            raise newException(ValueError, "匹配失败:"&this.str)
        while i < this.str.len:
            if this.str[i] in right:
                i+=1
            else:
                break
        this.index = i;
        # cond成立时,则包含当前字符x，否则不包含，截取的字符最少1字节
        return this.str.substr(found_start, found_end)
    raise newException(ValueError, "匹配失败:"&this.str)

proc parse_item_quote_string(this: var Line): string =
    var quote_start = -1
    while this.index < this.str.len:
        if quote_start < 0:
            if this.str[this.index] == '\32':
                this.index+=1
                continue
            elif this.str[this.index] == '\34':
                quote_start = this.index
                this.index+=1
                continue
            else:
                break
        if this.str[this.index] == '\34':
            let end_s = this.index - 1;
            this.index += 1
            # 不包含quote_start的位置，也不包含最后i的位置
            return this.str.substr(quote_start+1, end_s)
        else:
            this.index+=1
    raise newException(ValueError, "匹配失败:"&this.str)



# 仅数字
proc digital(x: char, y: char, z: char): bool = x >= '\48' and x <= '\57'

# 包含数字和.号
proc digital_dot(x: char, y: char, z: char): bool = (x >= '\48' and x <= '\57') or x == '\46'

# 包含数字字母和.号或:号（IPv4或IPv6）
proc digital_dot_colon(x: char, y: char, z: char): bool = (x >= '\48' and x <= '\58') or x == '\46' or (x >= '\97' and x <= '\122')

# 包含数字和.号或-号
proc digital_dot_minus(x: char, y: char, z: char): bool = (x >= '\48' and x <= '\57') or x == '\46' or x == '\45'

# 匹配到],并且下一个是空格
proc square_right_space(x: char, y: char, z: char): bool = not (x == '\93' and y == '\32')

# 非空格
proc not_space(x: char, y: char, z: char): bool = x != '\32'

# 当前字符是空格，上个字符是字母,不包含空格
proc string_end(x: char, y: char, z: char): bool = not (x == '\32' and ( (z >= '\65' and z <= '\90') or (z >= '\97' and z <= '\122')))

# 当前是空格，上一个是-或者数字
proc digital_or_none_end(x: char, y: char, z: char): bool = not (x == '\32' and ( (z >= '\48' and z <= '\57') or z == '\45'))

# 包含数字字母和.号或:号（IPv4或IPv6）
proc parse_remote_addr(this: var Line): string =
    return this.parse_item_trimx(blank, blank, digital_dot_colon)

# 去除可能存在的-,非空格
proc parse_remote_user(this: var Line): string =
    while this.index < this.str.len:
        case this.str[this.index]:
            of '\45':
                this.index+=1
            else:
                break;
    return this.parse_item_trimx(blank, blank, not_space)

# 匹配到],并且下一个是空格
proc parse_time_local(this: var Line): string =
    return this.parse_item_trimx(square_left, square_right, square_right_space)

# 匹配到双引号结束位置
proc parse_request_line(this: var Line): string =
    return this.parse_item_quote_string()

# 是数字
proc parse_status_code(this: var Line): string =
    return this.parse_item_trimx(blank, blank, digital)

# 是数字
proc parse_body_bytes_sent(this: var Line): string =
    return this.parse_item_trimx(blank, blank, digital)

# 匹配到双引号结束位置
proc parse_http_referer(this: var Line): string =
    return this.parse_item_quote_string()

# 匹配到双引号结束位置
proc parse_http_user_agent(this: var Line): string =
    return this.parse_item_quote_string()

# 匹配到双引号结束位置
proc parse_http_x_forwarded_for(this: var Line): string =
    return this.parse_item_quote_string()

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
    except CatchableError:
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
        except CatchableError:
            stderr.writeLine(line)
    # 分析完毕

    db.exec(sql"COMMIT;")

    echo &"共处理{total_lines}行,发送数据{formatSize(total_bytes_sent)},接收数据{formatSize(total_bytes_recv)}\n"


proc query(file: string, str: string) =
    if not file.fileExists:
        raise newException(DbError, "文件不存在:"&file)
    var line = ""
    var i = 0
    for row in open(file, "", "", "").fastRows(str.sql):
        i = 0
        line.setLen(0)
        for x in row:
            if i > 0:
                line.add('|')
            line.add(x)
            inc(i)
        echo line

try:
    let argc = paramCount()
    if argc > 1:
        query(paramStr(1), paramStr(2))
    elif argc > 0:
        process(paramStr(1))
    else:
        process(stdin)
except CatchableError:
    echo getCurrentExceptionMsg()
    quit(1)


