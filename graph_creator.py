import json
import matplotlib.pyplot as plt


def add_line(axs, res, o_index_name):
    ax1, ax2 = axs
    n_threads = [1, 2, 4, 8, 16, 32]
    mean1 = []
    lower = []
    upper = []
    mem_usage = []
    base_throughput = res[str(1)]['throughput']
    base_throughput_std = res[str(1)]['throughput_std']
    for n in n_threads:
        mem_usage.append(res[str(n)]['memory_usage'])
        throughput = res[str(n)]['throughput']
        throughput_std = res[str(n)]['throughput_std']
        mean1.append(throughput)
        mean1[-1] /= base_throughput
        lower.append(throughput - throughput_std)
        lower[-1] /= base_throughput - base_throughput_std
        upper.append(throughput + throughput_std)
        upper[-1] /= base_throughput + base_throughput_std
    if o_index_name == 'hot':
        color, std_color = 'blue', 'green'
    else:
        color, std_color = 'red', 'yellow'
    ax1.plot(n_threads, mean1, lw=2, label=o_index_name, color=color)
    ax1.fill_between(n_threads, lower, upper, facecolor=std_color, alpha=0.5,
                     label=o_index_name + '_std')
    ax2.plot(n_threads, mem_usage, lw=2, label=o_index_name, color=color)


def plot_bench(hot_res, masstree_res, bench_name):
    fig, axs = plt.subplots(2)
    add_line(axs, hot_res, 'hot')
    add_line(axs, masstree_res, 'masstree')
    axs[0].legend(loc='upper left')
    axs[0].set_xlabel('num threads')
    axs[0].set_ylabel('speedup')
    axs[1].legend(loc='upper left')
    axs[1].set_xlabel('num threads')
    axs[1].set_ylabel('memory usage')
    plt.savefig(f'{bench_name}.png')


def main():
    hot_res = json.load(open('res_hot.json', 'r'))
    masstree_res = json.load(open('res_masstree.json', 'r'))
    for bench in ['pred_bench', 'micro_bench', 'voter_bench']:
        plot_bench(hot_res[bench], masstree_res[bench], bench)


if __name__ == '__main__':
    main()
