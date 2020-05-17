#include "Keyfile.hpp"

#include <cassert>
#include <iostream>
using namespace std;
using namespace lemur::file;

int main(int argc, char *argv[])
{
    Keyfile btree;

    assert(argc == 2);
    cout << "Open: " << argv[1] << "\n";
    btree.open(argv[1]);

    string line;
    while (true)
    {
        // Prompt:
        cout << ">";
        cout.flush();

        line.clear();
        getline(cin, line);

        if (cin.eof())
        {
            break;
        }
        else if (cin.bad() || cin.fail())
        {
            break;
        }
        // startswith(x) == (rfind(x, 0) == 0)
        else if (line.rfind("get", 0) == 0)
        {
            assert(line[3] == ' ');
            string key = line.substr(4);
            int size = btree.getSize(key.c_str());
            if (size == -1)
            {
                cout << "MISS\n";
                continue;
            }
            cout << "FOUND len=" << size << "\n";
            assert(size >= 0);
            char *value = (char *)malloc(size);
            int actualSize = 0;
            if (btree.get(key.c_str(), value, actualSize, size))
            {
                cout << "FOUND " << value << "\n";
            }
        }
        else if (line == "close")
        {
            break;
        }
        else
        {
            cerr << line;
            assert(0);
        }
    }
    btree.close();
    return 0;
}