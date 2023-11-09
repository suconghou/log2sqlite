

parse nginx access log into sqlite database

解析的日志格式
```
    log_format vhosts '$remote_addr - $remote_user [$time_local] "$request" '
                      '$status $body_bytes_sent "$http_referer" '
                      '"$http_user_agent" "$http_x_forwarded_for" '
                      '$host $request_length $bytes_sent $upstream_addr '
                      '$upstream_status $request_time $upstream_response_time '
                      '$upstream_connect_time $upstream_header_time';


```

依赖 sqlite3 , alpine 内静态编译可 `apk add sqlite-dev sqlite-static`

以下三个版本均在`nimlang/nim:latest-alpine-slim`中编译

## nim 版本

```
nim --mm:arc -d:release --dynlibOverride:libsqlite3 --passC:-flto --passL:-flto --passL:-s --passL:-static --passL:-lsqlite3 --opt:speed c main
```


## c++ 版本


```
g++ -Wall -std=c++20 -O3 -lsqlite3 main.cpp

g++ -Wall -std=c++20 -flto=auto -static-libstdc++ -static-libgcc --static -Wl,-Bstatic,--gc-sections -O3 -ffunction-sections -fdata-sections main.cpp -lsqlite3 -o log2sqlite
```

## c 版本


mac
```
gcc -Wall -std=c17 -O3 -lsqlite3 main.c
```

linux 静态编译，因strptime函数的申明问题(https://stackoverflow.com/questions/43460876/trouble-including-function-declaration-for-strptime)

我们需要使用`gnu17`标准
```

gcc -Wall -std=gnu17 -flto=auto -static-libstdc++ -static-libgcc --static -Wl,-Bstatic,--gc-sections -O3 -ffunction-sections -fdata-sections main.c -lsqlite3 -o log2sqlite

```

注意： `-lsqlite3` 必须在`main.c`之后


每秒约处理10万条日志，c++版本比c版本慢约10%,nim版本和c版本速度相当


测试数据700MB+, 约200万行

| 版本  | 时间  |
| ------ | ------    |
| c     | 15.380s    |
| c++   | 18.942s    |
| nim   | 14.024s    |

可使用sqlite3命令对数据进行分析

```
sqlite3 nginx_log.db 'select count(1) as n,request from nginx_log group by request order by n desc limit 5;'


或者sql查询存储到sql文件

sqlite3 nginx_log.db < /tmp/1.sql 


sqlite3 nginx_log.db 'select count(1) as n,request,http_user_agent from nginx_log group by http_user_agent,request order by n desc limit 5;'

sqlite3 nginx_log.db 'select count(1) as n,request,remote_addr from nginx_log group by remote_addr,request order by n desc limit 5;'

```
