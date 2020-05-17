#include "BulkTree.hpp"

#include <cassert>
#include <iostream>
using namespace std;
using namespace indri::file;

int main(int argc, char *argv[]) {
    BulkTreeReader reader;

    assert(argc == 2);
    cout << "Open Read: " << argv[1] << "\n";
    reader.openRead(argv[1]);

    string line;
    while (true) {
        // Prompt:
        cout << ">";
        cout.flush();

        line.clear();
        getline(cin, line);

        if (cin.eof()) {
            break;
        } else if (cin.bad() || cin.fail()) {
            break;
        }

        // startswith(x) == (rfind(x, 0) == 0)
        if (line == "close") {
            break;
        } else if (line.rfind("get", 0) == 0) {
            assert(line[3] == ' ');
            string key = line.substr(4);
            
            BulkTreeIterator* iter = reader.findFirst(key.c_str());

            if (!iter || iter->finished()) {
                cout << "Iter not found\n";
                continue;
            }

            bool found = false;
            while (!iter->finished()) {
                iter->nextEntry();
                int valActual = 0;
                int keyActual = 0;
                char key_buffer[1024] = {0};
                char val_buffer[1024] = {0};
                // returns bool but never used!
                // DiskKeyfileVocabularyIterator
                iter->get(key_buffer, 1024, keyActual, val_buffer, 1024, valActual);
                if (keyActual > 0 && key == key_buffer) {
                    cout << "FOUND " << val_buffer << "\n";
                    found = true;
                    break;
                }
                //cout << "STATE " << keyActual << "," << valActual << " " << key_buffer << " -> " << val_buffer << "\n";
            }
            if (!found) {
                cout << "MISS\n";
            }
        } else {
            cerr << line;
            assert(0);
        }
    }
    reader.close();
    return 0;
}