from random import randrange

GROUPS = 5
GROUP_SIZE = 20
TIME_STEP = 300

with open("generated-data.txt", "w") as f:
    start = 0
    end = 500

    for _ in range(GROUPS):
        for _ in range(GROUP_SIZE):
            ida = 1 + randrange(10000)
            idb = 1 + randrange(10000)
            cpu = 1 + randrange(8)
            mem = 1 + randrange(32)
            vms = 1 + randrange(100)

            parts = [ida, idb, start, end, cpu, mem, vms]
            line = " ".join(str(x) for x in parts)
            f.write(line + '\n')

        start += TIME_STEP
        end += TIME_STEP
