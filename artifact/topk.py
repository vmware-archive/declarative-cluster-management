import argparse

scope_args = '"-n %d -f v2-cropped.txt -c 100 -m 200 -t 100 -s 100 -p %d -S -k %d"'
template = './gradlew runBenchmark --args=%s | sed $"s,\x1b\\[[0-9;]*[a-zA-Z],,g" &> %s'

def gen_cmd(root, method, n, p, k, i):
    outfile = "%s/%s-n%d-p%d-%d-%d.txt" % (root, method, n, p, k, i)
    args = scope_args % (n, p, k)
    cmd = template % (args, outfile)
    return cmd


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate benchmark bash.')
    parser.add_argument("-r", "--root", default="artifact/traces")
    args = parser.parse_args()

    # Generate all run commands 
    root = args.root
    f = open("scope_bench.sh", "w")
    n = 5000
    for method in ["scope"]:
        for p in [0, 50, 100]:
            for k in [50, 75, 100, 150, 200, 400, 800, 1600]:
                for i in range(1):
                    cmd = gen_cmd(root, method, n, p, k, i)
                    f.write('%s\n\n' % cmd)
    f.close()