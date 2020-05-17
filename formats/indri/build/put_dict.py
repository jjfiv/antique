import subprocess
import sys

PUTS = []

with open("../../../data/vocab.txt") as dictionary:
    for line in dictionary:
        word = line.strip().lower()
        cmd = "put {} {}\n".format(word, len(word))
        PUTS.append(cmd);

PUTS.append("close\n");

p = subprocess.Popen(["./write_bulktree", "dict.bulktree"], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
p.communicate(input=''.join(PUTS).encode('UTF-8'));

p = subprocess.Popen(["./write_keyfile", "dict.keyfile"], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
p.communicate(input=''.join(PUTS).encode('UTF-8'));
