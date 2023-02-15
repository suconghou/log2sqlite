

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

## c++ 版本


```
g++ -Wall -std=c++20 -O3 -lsqlite3 main.cpp

g++ -Wall -std=c++20 -static-libstdc++ -static-libgcc --static -Wl,-Bstatic,--gc-sections -O3 -ffunction-sections -fdata-sections main.cpp -lsqlite3 -o log2sqlite
```

## c 版本


mac
```
gcc -Wall -std=c11 -O3 -lsqlite3 main.c
```

linux 静态编译，因strptime函数的申明问题(https://stackoverflow.com/questions/43460876/trouble-including-function-declaration-for-strptime)

我们需要使用`gnu11`标准
```

gcc -Wall -std=gnu11 -static-libstdc++ -static-libgcc --static -Wl,-Bstatic,--gc-sections -O3 -ffunction-sections -fdata-sections main.c -lsqlite3 -o log2sqlite

```

注意： `-lsqlite3` 必须在`main.c`之后


每秒约处理10万条日志，c++版本比c版本慢约10%

