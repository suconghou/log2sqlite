#include <string>
#include <fstream>
#include <sstream>
#include <iostream>
#include <set>
#include "process.cpp"

using namespace std;

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
            return process(cin);
        }
        // ifstream是输入文件流（input file stream）的简称, std::ifstream
        // 离开作用域后，fh文件将被析构器自动关闭
        ifstream fh(argv[1]); // 打开一个文件
        if (!fh)
        {
            // open file failed
            perror(argv[1]);
            return 1;
        }
        return process(fh);
    }
    catch (char *e)
    {
        fprintf(stderr, "%s\n", e);
        return 2;
    }
}