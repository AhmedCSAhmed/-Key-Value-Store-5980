import argparse
import queue
import threading
import time

import requests
from requests.adapters import HTTPAdapter


PRINT_INTERVAL = 3
thread_local = threading.local()


def build_session(pool_size, timeout):
    session = requests.Session()
    session.trust_env = False
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_session(pool_size, timeout):
    session = getattr(thread_local, "session", None)
    if session is None:
        session = build_session(pool_size, timeout)
        thread_local.session = session
    return session


def discover_nodes(base_port, num_servers, timeout):
    requested_nodes = [f"http://127.0.0.1:{base_port + i}" for i in range(num_servers)]
    healthy_nodes = []

    for node_url in requested_nodes:
        try:
            response = requests.get(f"{node_url}/healthz", timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            healthy_nodes.append(
                {
                    "name": payload.get("node", node_url),
                    "url": node_url,
                }
            )
        except requests.RequestException as exc:
            print(f"Skipping unreachable node {node_url}: {exc}")

    return healthy_nodes


def kv_store_operation(session, timeout, op_type, key, value=None, node=None):
    base_url = node["url"]
    try:
        if op_type == "set":
            response = session.post(f"{base_url}/{key}", json={"value": value}, timeout=timeout)
        elif op_type == "get":
            response = session.get(f"{base_url}/{key}", timeout=timeout)
        else:
            raise ValueError("invalid operation type")

        response.raise_for_status()
        return True
    except Exception as exc:
        print(f"Error during {op_type} operation for key '{key}' on {base_url}: {exc}")
        return False


def worker_thread(operations_queue, recent_latencies, stats_lock, node_stats, total_stats, start_event, pool_size, timeout):
    start_event.wait()
    session = get_session(pool_size, timeout)

    while True:
        item = operations_queue.get()
        if item is None:
            operations_queue.task_done()
            return

        op_type, key, value, node_index = item
        node = node_stats[node_index]

        start_time = time.perf_counter()
        success = kv_store_operation(session, timeout, op_type, key, value, node)
        latency = time.perf_counter() - start_time

        with stats_lock:
            if success:
                total_stats["successes"] += 1
                total_stats["latency_sum"] += latency
                node["successes"] += 1
                node["latency_sum"] += latency
                recent_latencies.put(latency)
            else:
                total_stats["failures"] += 1
                node["failures"] += 1

        operations_queue.task_done()


def monitor_performance(recent_latencies, total_stats, stats_lock, done_event):
    last_print = time.perf_counter()

    while not done_event.is_set() or not recent_latencies.empty():
        time.sleep(PRINT_INTERVAL)
        current_time = time.perf_counter()
        elapsed_time = current_time - last_print

        latencies = []
        while True:
            try:
                latencies.append(recent_latencies.get_nowait())
            except queue.Empty:
                break

        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            throughput = len(latencies) / elapsed_time if elapsed_time > 0 else float("nan")
            with stats_lock:
                failures = total_stats["failures"]
            print(
                f"[Last {PRINT_INTERVAL} seconds] Throughput: {throughput:.2f} ops/sec, "
                f"Avg Latency: {avg_latency:.5f} sec/op, Failures: {failures}"
            )

        last_print = time.perf_counter()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8090, help="base port of the first KV node")
    parser.add_argument("--num-servers", type=int, default=3, help="number of KV nodes to probe")
    parser.add_argument("--threads", type=int, default=12, help="number of worker threads")
    parser.add_argument("--ops-per-thread", type=int, default=1000, help="set operations per thread; get operations match this count")
    parser.add_argument("--timeout", type=float, default=2.0, help="per-request timeout in seconds")
    parser.add_argument("--require-all-nodes", action="store_true", help="fail if any requested node is unreachable")
    args = parser.parse_args()

    healthy_nodes = discover_nodes(args.port, args.num_servers, args.timeout)
    if not healthy_nodes:
        raise SystemExit("No healthy KV nodes were found. Start the servers before running the benchmark.")
    if args.require_all_nodes and len(healthy_nodes) != args.num_servers:
        raise SystemExit(
            f"Expected {args.num_servers} healthy nodes, but found {len(healthy_nodes)}. "
            "Fix the cluster startup before benchmarking."
        )

    operations_queue = queue.Queue()
    recent_latencies = queue.Queue()
    stats_lock = threading.Lock()
    total_stats = {
        "successes": 0,
        "failures": 0,
        "latency_sum": 0.0,
    }
    node_stats = {
        idx: {
            "name": node["name"],
            "url": node["url"],
            "successes": 0,
            "failures": 0,
            "latency_sum": 0.0,
        }
        for idx, node in enumerate(healthy_nodes)
    }

    total_requested_ops = args.threads * args.ops_per_thread * 2
    for i in range(args.threads * args.ops_per_thread):
        key = f"key_{i}"
        value = f"value_{i}"
        node_idx = i % len(healthy_nodes)
        operations_queue.put(("set", key, value, node_idx))

    for j in range(args.threads * args.ops_per_thread):
        key = f"key_{j}"
        node_idx = j % len(healthy_nodes)
        operations_queue.put(("get", key, None, node_idx))

    for _ in range(args.threads):
        operations_queue.put(None)

    start_event = threading.Event()
    done_event = threading.Event()

    threads = [
        threading.Thread(
            target=worker_thread,
            args=(
                operations_queue,
                recent_latencies,
                stats_lock,
                node_stats,
                total_stats,
                start_event,
                args.threads,
                args.timeout,
            ),
        )
        for _ in range(args.threads)
    ]

    monitoring_thread = threading.Thread(
        target=monitor_performance,
        args=(recent_latencies, total_stats, stats_lock, done_event),
        daemon=True,
    )
    monitoring_thread.start()

    start_time = time.perf_counter()
    for thread in threads:
        thread.start()

    start_event.set()
    operations_queue.join()
    done_event.set()

    for thread in threads:
        thread.join()

    total_time = time.perf_counter() - start_time
    successful_ops = total_stats["successes"]
    failed_ops = total_stats["failures"]
    throughput = successful_ops / total_time if total_time > 0 else float("nan")
    average_latency = (
        total_stats["latency_sum"] / successful_ops if successful_ops else float("nan")
    )

    print("\nFinal Results:")
    print(f"Requested operations: {total_requested_ops}")
    print(f"Successful operations: {successful_ops}")
    print(f"Failed operations: {failed_ops}")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} successful operations per second")
    print(f"Average Latency: {average_latency:.5f} seconds per successful operation")

    print("\nPer-Node Final Results:")
    for idx, node in node_stats.items():
        successes = node["successes"]
        failures = node["failures"]
        avg_node_latency = node["latency_sum"] / successes if successes else float("nan")
        node_throughput = successes / total_time if total_time > 0 else float("nan")

        print(f"\nNode {idx + 1} ({node['name']} @ {node['url']}):")
        print(f"Successful operations: {successes}")
        print(f"Failed operations: {failures}")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Throughput: {node_throughput:.2f} successful operations per second")
        print(f"Average Latency: {avg_node_latency:.5f} seconds per successful operation")


if __name__ == "__main__":
    main()
