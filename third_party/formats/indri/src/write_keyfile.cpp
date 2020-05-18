#include "Keyfile.hpp"

#include <cassert>
#include <iostream>
using namespace std;
using namespace lemur::file;

int main(int argc, char *argv[])
{
    Keyfile btree;

    assert(argc == 2);
    cout << "Create: " << argv[1] << "\n";
    btree.create(argv[1]);

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
        if (line == "close")
        {
            break;
        }
        else if (line.rfind("put", 0) == 0)
        {
            assert(line[3] == ' ');
            size_t split = line.find(' ', 4);
            assert(split != string::npos);
            string key = line.substr(4, split - 4);
            string value = line.substr(split + 1);
            cout << "Putting: '" << key << "' -> '" << value << "'\n";
            btree.put(key.c_str(), value.c_str(), value.length());
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