import subprocess
import sys


def allow_time(p):
    try:
        # give it 50 ms to wrap-up
        p.communicate(timeout=0.050)
        return True
    except TimeoutExpired:
        return False

PUTS = []

with open("/usr/share/dict/words") as dictionary:
    for line in dictionary:
        word = line.strip().lower()
        cmd = "put {} {}\n".format(word, len(word))
        PUTS.append(cmd);

PUTS.append("close\n");

p = subprocess.Popen(["./write_bulktree", "dict.bulktree"], stdout=subprocess.PIPE, stdin=subprocess.PIPE)
p.communicate(input=''.join(PUTS).encode('UTF-8'));

