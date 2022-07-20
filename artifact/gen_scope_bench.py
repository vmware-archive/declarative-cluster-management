import argparse

scope_args = '"-n %d -f v2-cropped.txt -c 100 -m 200 -t 100 -s 100 -p %d -S"'
orig_args = '"-n %d -f v2-cropped.txt -c 100 -m 200 -t 100 -s 100 -p %d"'
template = './gradlew runBenchmark --args=%s | sed $"s,\x1b\\[[0-9;]*[a-zA-Z],,g" &> %s'

def gen_cmd(root, method, n, p, i):
    outfile = "%s/%s-n%d-p%d-%d.txt" % (root, method, n, p, i)
    if method == 'orig':
        args = orig_args % (n, p)
    else:
        args = scope_args % (n, p)
    cmd = template % (args, outfile)
    return cmd


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate benchmark bash.')
    parser.add_argument("-r", "--root", default="traces/")
    args = parser.parse_args()

    # Generate all run commands 
    root = args.root
    f = open("scope_bench.sh", "w")
    for method in ["scope"]:
        for n in [500, 5000, 50000]:
            for p in [0, 50, 100]:
                for i in range(5):
                    cmd = gen_cmd(root, method, n, p, i)
                    f.write('%s\n\n' % cmd)
    f.close()