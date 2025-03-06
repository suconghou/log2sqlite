#include <string>
#include <fstream>
#include <sstream>
#include <iostream>
#include <set>
#include "process.cpp"

int main(int argc, char *argv[])
{
    try
    {
        if (argc > 2)
        {
            return query(argv[1], argv[2]);
        }
        else if (argc < 2)
        {
            return process(std::cin);
        }
        // ifstream是输入文件流（input file stream）的简称, std::ifstream
        // 离开作用域后，fh文件将被析构器自动关闭
        std::ifstream fh(argv[1]); // 打开一个文件
        if (!fh)
        {
            // open file failed
            perror(argv[1]);
            return 1;
        }
        char buf[81920];
        fh.rdbuf()->pubsetbuf(buf, sizeof(buf));
        return process(fh);
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
        return 2;
    }
}