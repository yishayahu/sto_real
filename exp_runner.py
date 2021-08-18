import json
import subprocess
import numpy as np
import os
import plac
import  resource


def run_cmd(cmd):


    p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    curr_mem_usage = -1
    while p.poll() is None:
        res = subprocess.run(f"cat /proc/{p.pid}/statm".split(), stdout=subprocess.PIPE)
        res = int(res.stdout.decode("utf-8").split()[0])
        if res > curr_mem_usage:
            curr_mem_usage = res

    return p.stdout.read().decode("utf-8").split('\n'),curr_mem_usage
def make_bench(bench):
    cmd = f'make {bench}'
    run_cmd(cmd)



def run(bench, n_threads):
    cmd = f'./{bench} --nthreads={n_threads}'
    cmd_output, cmd_mem_usage = run_cmd(cmd)
    throughput = [float(x.split()[1]) for x in cmd_output if 'Throughput' in x]
    elapsed_time = [float(x.split()[2]) for x in cmd_output if 'Elapsed time' in x]
    return throughput[-1], elapsed_time[-1],cmd_mem_usage


def main(o_index, clean: ('', 'flag', 'c'), use_exists: ('', 'flag', 'u')):
    print(f'mounting hugepges  ...')
    os.system('./mount_hugepages.sh 40000')
    print(f'running {o_index} ...')
    if clean:
        print('running make clean ...')
        make_bench('clean')
    res_dict = {}
    if use_exists:
        if os.path.exists(f'res_{o_index}.json'):
            print('loading previous results')
            res_dict = json.load(open(f'res_{o_index}.json', 'r'))
    for bench in ['pred_bench', 'micro_bench', 'voter_bench']:
        print(f'running make {bench} ...')
        if bench not in res_dict:
            res_dict[bench] = {}

        make_bench(bench)
        base_throughput = 1
        for n_thread in [1, 2, 4, 8, 16, 32]:

            if str(n_thread) in res_dict[bench]:
                continue
            print(f'running ./{bench} --nthread={n_thread} for 30 times...')
            temporary_res_t = []
            temporary_res_e = []
            temporary_res_m = []
            for i in range(30):
                throughput, elapsed_time,memory_usage = run(bench, n_thread)

                temporary_res_t.append(throughput)
                temporary_res_m.append(memory_usage)
                temporary_res_e.append(elapsed_time)
            throughput_mean, elapsed_time_mean, throughput_std, elapsed_time_std = np.mean(
                temporary_res_t), np.mean(temporary_res_e), np.std(temporary_res_t), np.std(temporary_res_e)
            if n_thread == 1:
                base_throughput = throughput_mean
            res_dict[bench][n_thread] = {'throughput': throughput_mean, "throughput_std": throughput_std,
                                         'elapsed_time': elapsed_time_mean,
                                         "elapsed_time_std": elapsed_time_std,
                                         'speedup': throughput_mean / base_throughput,
                                         "memory_usage":np.mean(temporary_res_m)}
            print(f'results are in res_{o_index}.json')
            json.dump(res_dict, open(f'res_{o_index}.json', 'w'))


if __name__ == '__main__':
    plac.call(main)
