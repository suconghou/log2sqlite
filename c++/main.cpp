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
            return process(stdin);
        }
        FILE *fh = fopen(argv[1], "r");
        if (!fh)
        {
            // open file failed
            perror(argv[1]);
            return 1;
        }
        return process(fh);
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << std::endl;
        return 2;
    }
}