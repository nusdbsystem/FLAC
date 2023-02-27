import json
import subprocess
import os
import time

local = True
pool = []
run_server = "docker exec -i participant ./bin/FLAC-server -node=co -preload -addr="
run_rl_server_cmd = "python3 down/main.py "
protocols = ["flac", "3pc", "2pc"]
logf = open("./tmp/progress.log", "w")

if local:
    run_client_cmd = "./bin/FLAC-server -node=ca -local=true -addr="
else:
    run_client_cmd = "docker exec -i coordinator ./bin/FLAC-server -node=ca -addr="


def get_server_cmd(addr, r, minlevel, env, nf):
    cmd = run_server + str(addr) + \
          " -r=" + str(r) + \
          " -tl=" + str(env) + \
          " -nf=" + str(nf) + \
          " -ml=" + str(minlevel)
    return cmd


def get_client_cmd(bench, protocol, clients, r, file, env=20, alg=1, nf=-1, ml=0, skew=0.5, cross=100, len=5, np=3):
    return run_client_cmd + " -bench=" + str(bench) + \
           " -p=" + str(protocol) + \
           " -c=" + str(clients) + \
           " -d=" + str(alg) + \
           " -nf=" + str(nf) + \
           " -tl=" + str(env) + \
           " -ml=" + str(ml) + \
           " -skew=" + str(skew) + \
           " -cross=" + str(cross) + \
           " -len=" + str(len) + \
           " -part=" + str(np) + \
           " -r=" + str(r) + file


if local:
    with open("./configs/local.json") as f:
        config = json.load(f)
else:
    with open("./configs/network.json") as f:
        config = json.load(f)

for id_ in config["coordinators"]:
    run_client_cmd = run_client_cmd + config["coordinators"][id_]


# gcloud beta compute ssh --zone "asia-southeast1-a" "cohort1" -- '
def execute_cmd_in_gcloud(zone, instance, cmd):
    cmd = "gcloud beta compute ssh --zone " + "%s %s -- \'" % (zone, instance) + " " + cmd + "\'"
    ssh = subprocess.Popen(cmd,
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    return ssh


def execute_cmd_in_ssh(address, cmd):
    cmd = "ssh tmp@" + address + " " + cmd
    print(cmd)
    ssh = subprocess.Popen(cmd,
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
    return ssh


def run_task(cmd):
    print(cmd)
    print(cmd, file=logf)
    logf.flush()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                         shell=True, preexec_fn=os.setsid)
    return p


def start_participant(service, r, minlevel, env, nf):
    cmd = get_server_cmd(service, r, minlevel, env, nf)
    print(cmd)
    return execute_cmd_in_ssh(service.split(':')[0], cmd)


#    return execute_cmd_in_gcloud(zone, instance, cmd)

def start_service_on_all(r, run_rl=False, time=0, minlevel=1, env=25, nf=-1, num=3):
    if run_rl:
        pool.append(run_task(run_rl_server_cmd + str(time) + ">./tmp/train.log"))
    if local:
        return
    for id_ in config["participants"]:
        if id_ == "1":
            pool.append(
                start_participant(config["participants"][id_], r, minlevel,
                                  env, nf))
        elif int(id_) <= num:
            pool.append(
                start_participant(config["participants"][id_], r, minlevel,
                                  25, -1))


def terminate_service():
    global pool
    for p in pool:
        p.wait()
    pool = []


def delete_extra_zero(n):
    if isinstance(n, int):
        return str(n)
    if isinstance(n, float):
        n = str(n).rstrip('0')
        if n.endswith('.'):
            n = n.rstrip('.')
        return n
    return "nooo"

def run_all_heu(alg, env, bench="tpc", c=3000, r=1, nf=-1):
    if env <= 0:
        filename = ">./tmp/he/CF-all-" + str(-env) + "-" + str(alg) + ".log"
    else:
        filename = ">./tmp/he/NF-all-" + str(nf) + "-" + str(alg) + ".log"
    for po, d in [("3pc", -1), ("easy", -1), ("pac", -1), ("2pc", -1), ("flac", 0)]:
        for each in range(TestBatch):
            start_service_on_all(r, run_rl=(alg == 0), time=3 * max(-env, nf) + 5 + 5, env=env, nf=nf)
            time.sleep(2)
            p = run_task(get_client_cmd(bench, po, c, r, filename, env, alg, nf))
            p.wait()
            terminate_service()
            if filename[1] == '.':
                filename = ">" + filename

def run_heu(alg, env, bench="tpc", c=3000, r=1, nf=-1):
    if env <= 0:
        filename = ">./tmp/he/CF-" + str(-env) + "-" + str(alg) + ".log"
    else:
        filename = ">./tmp/he/NF-" + str(nf) + "-" + str(alg) + ".log"

    for each in range(TestBatch):
        start_service_on_all(r, run_rl=(alg == 0), time=3 * max(-env, nf) + 5 + 5, env=env, nf=nf)
        time.sleep(2)
        p = run_task(get_client_cmd(bench, "flac", c, r, filename, env, alg, nf))
        p.wait()
        terminate_service()
        if filename[1] == '.':
            filename = ">" + filename


def run_exp_loose_failure_free(bench, r):
    l = [1] + [2 ** c for c in range(8, 13, 1)]
    if bench == "ycsb":
        l = [1, 16, 64, 128, 256, 512]
    for c in l:
        filename = ">./tmp/ff/" + bench.upper() + str(c) + ".log"
        rnd = TestBatch

        for po, lv in [("3pc", -1), ("easy", -1), ("pac", -1), ("2pc", -1), ("flac", 0)]:
            for each in range(rnd):
                start_service_on_all(r)
                time.sleep(1)
                p = run_task(get_client_cmd(bench, po, c, r, filename, ml=lv))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename


def run_exp_loose_crash_failure(bench, r):
    l = [1] + [2 ** c for c in range(8, 13, 1)]
    if bench == "ycsb":
        l = [1, 64, 128, 256, 512, 1024]
    for c in l:
        filename = ">./tmp/cf/" + bench.upper() + str(c) + ".log"
        rnd = TestBatch

        for po, lv in [("3pc", -1), ("easy", -1), ("pac", -1), ("2pc", -1), ("flac", 0)]:
            for each in range(rnd):
                start_service_on_all(r)
                time.sleep(1)
                p = run_task(get_client_cmd(bench, po, c, r, filename, cross=50, ml=lv, env=0))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename


def run_exp_sensitive_crash_failure(r):
    c = 512
    rnd = TestBatch

    for cross in [0, 20, 40, 50, 60, 80, 100]:
        filename = ">./tmp/cf/" + "cross" + str(cross) + ".log"
        for po, lv in [("3pc", -1), ("easy", -1), ("pac", -1), ("2pc", -1), ("flac", 0)]:
            for each in range(rnd):
                start_service_on_all(r, env=0)
                time.sleep(1)
                p = run_task(get_client_cmd("ycsb", po, c, r, filename, ml=lv, cross=cross, env=0))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename


def run_skew():
    c = 512
    r = 1
    rnd = TestBatch
    for sk in [0.0, 0.2, 0.4, 0.6, 0.8]:
        filename = ">./tmp/ff/" + "skew" + str(sk) + ".log"
        for po, lv in [("3pc", -1), ("easy", -1), ("pac", -1), ("2pc", -1), ("flac", 0)]: # fixed level
            for each in range(rnd):
                start_service_on_all(r)
                time.sleep(1)
                p = run_task(get_client_cmd("ycsb", po, c, r, filename, ml=lv, skew=sk))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename


def run_sensitive():
    c = 512
    r = 1
    rnd = TestBatch
    for cross in [0, 20, 40, 50, 60, 80, 100]:
        filename = ">./tmp/ff/" + "cross" + str(cross) + ".log"
        for po, lv in [("3pc", -1), ("easy", -1), ("pac", -1), ("2pc", -1), ("flac", 0)]: # fixed level
            for each in range(rnd):
                start_service_on_all(r)
                time.sleep(1)
                p = run_task(get_client_cmd("ycsb", po, c, r, filename, ml=lv, cross=cross))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename


def run_micro_heu():
    run_all_heu(0, -10)
    run_all_heu(0, -1000)
    for t in [100]:
        for i in [0, 1, 1024]:
            run_heu(i, -t)
            run_heu(i, 20, nf=t)


def run_exp_additional(bench, r):
    l = [1] + [2 ** c for c in range(8, 13, 1)]
    if bench == "ycsb":
        l = [1, 64, 128, 256, 512, 1024]
    for c in l:
        filename = ">./ff/addition" + str(c) + ".log"
        rnd = TestBatch

        for po, lv in [("flac", 1), ("flac", 2), ("flac", 3)]:
            for each in range(rnd):
                start_service_on_all(r)
                time.sleep(1)
                p = run_task(get_client_cmd(bench, po, c, r, filename, ml=lv))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename

def run_scale():
    c = 512
    rnd = TestBatch
    for num in [3, 5, 7, 10, 15]:
        filename = ">./tmp/ff/" + "scale" + str(num) + ".log"
        r = num/3
        for po, lv in [("2pc", -1), ("flac", -1)]: # fixed level
            for each in range(rnd):
                start_service_on_all(r, num=num)
                time.sleep(1)
                p = run_task(get_client_cmd("ycsb", po, c, r, filename, ml=lv, len=0, np=num, skew=0.5))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename

def test_conn():
    filename = ">./test_sk.log"
    for po, _ in [("easy", -1), ("flac", -1)]:
        # [("3pc", -1), ("easy", -1), ("pac", -1), ("2pc", -1), ("flac", -1)]
        for rnd in range(TestBatch):
            start_service_on_all(1)
            time.sleep(1)
            p = run_task(get_client_cmd("ycsb", po, 512, 1, filename, skew=0))
            p.wait()
            terminate_service()
            if filename[1] == '.':
                filename = ">" + filename

    filename = ">./test_cl.log"
    for po, _ in [("easy", -1), ("flac", -1)]:
        # [("3pc", -1), ("easy", -1), ("pac", -1), ("2pc", -1), ("flac", -1)]
        for rnd in range(TestBatch):
            start_service_on_all(1)
            time.sleep(1)
            p = run_task(get_client_cmd("ycsb", po, 50, 1, filename, skew=0.5))
            p.wait()
            terminate_service()
            if filename[1] == '.':
                filename = ">" + filename

def run_exp_r():
    l = range(100, 4000, 300)
    for c in l:
        filename = ">./tmp/ff/" + "EXPR"  + str(c) + ".log"
        rnd = TestBatch

        for r in [0.1, 0.2, 0.5, 1, 2, 3, 4, 6, 8]:
            for each in range(rnd):
                start_service_on_all(r)
                time.sleep(1)
                p = run_task(get_client_cmd("tpc", "flac", c, r, filename, ml=0))
                p.wait()
                terminate_service()
                if filename[1] == '.':
                    filename = ">" + filename

def run_fixed_heu(alg, env, bench="ycsb", c=512, r=1, nf=-1):
    if env <= 0 and nf == -1:
        filename = ">./tmp/he/CF-fixed-" + str(-env) + "-" + str(alg) + ".log"
    elif env > 0:
        filename = ">./tmp/he/NF-fixed-" + str(nf) + "-" + str(alg) + ".log"
    else:
        filename = ">./tmp/he/MX-fixed-" + str(nf) + "-" + str(alg) + ".log"
    for po, lv in [("flac", -1), ("flac", 0)]:
        for each in range(TestBatch):
            start_service_on_all(r, run_rl=(alg == 0), time=3 * max(-env, nf) + 5 + 5, env=env, nf=nf)
            time.sleep(2)
            p = run_task(get_client_cmd(bench, po, c, r, filename, env, alg, nf, ml=lv))
            p.wait()
            terminate_service()
            if filename[1] == '.':
                filename = ">" + filename

def run_fixed():
#    run_fixed_heu(0, 20, nf=1000)
    run_fixed_heu(0, 20, nf=1)

TestBatch = 10

if __name__ == '__main__':
     test_conn()
#    run_scale()
#    run_skew()
#    run_sensitive()
#    run_exp_loose_failure_free("ycsb", 1)
#    run_exp_sensitive_crash_failure(1)
#    run_scale_li()
#    run_fixed()
#    run_exp_r()
#    run_exp_loose_failure_free("tpc", 1)
#    run_exp_additional("tpc", 1)
#    run_debug()
#    run_micro_heu()
#    logf.close()
