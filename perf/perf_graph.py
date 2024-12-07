import matplotlib.pyplot as plt


def push_graph():
    push_data = [
        [1, 1, 0.2775759696960449],
        [1, 5, 0.7206840515136719],
        [2, 10, 0.7824387550354004],
        [4, 20, 0.9902811050415039],
        [8, 40, 0.9699010848999023],
    ]

    tasks = [str((el[0], el[1],)) for el in push_data]
    times = [el[2] for el in push_data]

    plt.bar(tasks, times)
    plt.title("Push Graph Scaling Study")
    plt.xlabel("(Num Workers, Num Tasks)")
    plt.ylabel("Time to Complete All Tasks (seconds)")
    plt.savefig("perf/push_perf.png")
    plt.close()

def pull_graph():
    pull_data = [
        [ 1, 0.05316281318664551],
        [ 5, 0.0624079704284668],
        [10, 0.12756609916687012],
        [20, 0.25497007369995117],
        [40, 0.4865279197692871],
    ]
    tasks = [str(el[0]) for el in pull_data]
    times = [el[1] for el in pull_data]
    plt.bar(tasks, times)
    plt.title("Pull Graph Scaling Study")
    plt.xlabel("Number of Tasks")
    plt.ylabel("Time to Complete All Tasks (seconds)")
    plt.savefig("perf/pull_perf.png")
    plt.close()

def local_graph():
    local_data = [
        [1, 0.0065097808837890625],
        [5, 0.015331268310546875],
        [10, 0.027924299240112305],
        [20, 0.05584883689880371],
    ]
    tasks = [str(el[0]) for el in local_data]
    times = [el[1] for el in local_data]
    plt.bar(tasks, times)
    plt.title("Local Graph Scaling Study")
    plt.xlabel("Number of Tasks")
    plt.ylabel("Time to Complete All Tasks (seconds)")
    plt.savefig("perf/local_perf.png")
    plt.close()

if __name__ == "__main__":
    push_graph()
    pull_graph()
    local_graph()