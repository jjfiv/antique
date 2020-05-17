#include "BulkTree.hpp"

using indri::file::BulkTreeWriter;
using indri::file::BulkTreeReader;

int main(int argc, char *argv[]) {
    BulkTreeWriter writer;

    writer.create("test.bulktree");
    const char data[] = "example";
    writer.put("a", data, sizeof(data));

    writer.close();

    return 0;
}