import multiprocessing
import sys

from splunktalib.common import log
import kafka_consts as c


logger = log.Logs().get_logger("main")


def expand_or_merge_tasks(task_configs, kafka_clusters):
    cpu_count = multiprocessing.cpu_count()
    logger.info("initial_task_size=%s", len(task_configs))
    if len(task_configs) < (cpu_count - 2) * 2:
        logger.info("final_task_size=%s", len(task_configs))
        return task_configs

    final_tasks = expand_or_merge_tasks_by_cpu(
        task_configs, kafka_clusters, cpu_count)

    logger.info("final_task_size=%s", len(final_tasks))
    return final_tasks


def expand_or_merge_tasks_by_cpu(task_configs, kafka_clusters, cpu_count):
    """
    The idea is:
        1. Given a bunch of kafka data collection tasks with traffic load set,
           the tasks may be for different kafka clusters.
        2. Give a bunch of CPU cores
        3. Sort out how to split the high traffic tasks into small ones, or
           merge low traffic tasks of the same kafka clusters to a big one

    The algorithm is:
        1. Categorize all of the tasks according to Kafka Cluster
        2. Split HighTrafficTopic traffic tasks into small ones
        3. Merge tasks of the same topic group into big one
        4. Reserve 200 % CPU for splunkd
        5. The final task number should not be too over committed, which should
           not be more than 1.5 * (cpu_count - 2) tasks
    """

    def _collect_tasks(tasks):
        merged_tasks = []
        for group_tasks in tasks.itervalues():
            merged_tasks.extend(group_tasks.values())
        return merged_tasks

    reserved_cpu_count = cpu_count - 2
    tasks_by_group = _merge_tasks_by_topic_group(task_configs, False)
    merged_tasks = _collect_tasks(tasks_by_group)
    total_task = len(merged_tasks)
    if total_task < reserved_cpu_count:
        logger.info("total_task=%s, reserved_cpu=%s, CPU usage "
                    "may not be fully utilized", total_task,
                    reserved_cpu_count)
    elif total_task > 2 * reserved_cpu_count:
        logger.info("total_task=%s, reserved_cpu=%s, CPU usage "
                    "may be over committed", total_task,
                    reserved_cpu_count)
    return merged_tasks


def _merge_tasks_by_topic_group(task_configs, merge_none_topic_group=True):
    """
    by cluster, then topic group
    """

    tasks_by_topic_group = {}
    for task in task_configs:
        if task[c.kafka_brokers] not in tasks_by_topic_group:
            tasks_by_topic_group[task[c.kafka_brokers]] = {}

        cluster_tasks = tasks_by_topic_group[task[c.kafka_brokers]]
        topic_group = task.get(c.kafka_topic_group)
        if not merge_none_topic_group and not topic_group:
            cluster_tasks[task[c.name]] = task
            continue

        if topic_group not in cluster_tasks:
            cluster_tasks[topic_group] = None

        if cluster_tasks[topic_group] is not None:
            cluster_tasks[topic_group][c.kafka_topic].extend(
                task[c.kafka_topic])
        else:
            cluster_tasks[topic_group] = task

    return tasks_by_topic_group


def _merge_tasks_by_cluster(tasks_by_topic_group):
    tasks_by_cluster = []
    for cluster, group_tasks in tasks_by_topic_group.iteritems():
        merged_tasks = []
        for task in group_tasks.itervalues():
            merged_tasks.extend(task[c.kafka_topic])
        merged_task = group_tasks.values()[0]
        merged_task[c.kafka_topic] = merged_tasks
        tasks_by_cluster.append(merged_task)

    return tasks_by_cluster


def get_task_assignments(clusters, forwarders,
                         kafka_topics, dispatched_tasks):
    """
    clusters: {kafka_stanza_name: kafka_stanza_dict}
    forwarders: {forwarder_stanza_name: forwarder_stanza_dict}
    kafka_toipcs: {
        kafka_stanza_name: {kafka_topic: set(...), kafka_partition: xxx}
    }
    dispatched_tasks: {
        kafka_stanza_name: {
            forwarder_stanza_name: {
                "brokers": [brokers],
                "kafka_topic": [[topic, partitions, index],...],
            },
        }
    }
    :return: same schema as dispatched_tasks
    """

    topic_count = sum((len(t[c.kafka_topic])
                       for t in kafka_topics.itervalues()))
    forwarder_count = len(forwarders)
    logger.info("Discover %d topic/partitions and there are %d forwarders",
                topic_count, forwarder_count)
    if forwarder_count == 0:
        return

    topic_group_2_forwarders = {}
    current_shares = _get_shares(
        clusters, forwarders, kafka_topics, dispatched_tasks,
        topic_group_2_forwarders)

    task_assignments = {}
    for kafka_stanza, topic_partitions in kafka_topics.iteritems():
        index = clusters[kafka_stanza].get(c.index)
        task_assignments[kafka_stanza] = {}
        for topic in topic_partitions[c.kafka_topic]:
            forwarder = _pick_forwarder(
                current_shares, topic_group_2_forwarders,
                clusters, kafka_stanza)
            if forwarder not in task_assignments[kafka_stanza]:
                task_assignments[kafka_stanza][forwarder] = {
                    c.kafka_brokers: clusters[kafka_stanza][c.kafka_brokers],
                    c.kafka_topic: [],
                }
            assigned = task_assignments[kafka_stanza][forwarder][c.kafka_topic]
            assigned.append(
                [topic, topic_partitions[c.kafka_partition], index])

    for cluster_stanza, task in task_assignments.items():
        if not task:
            del task_assignments[cluster_stanza]
    logger.debug("Task assignments=%s", task_assignments)
    return task_assignments


def _pick_forwarder(current_shares, topic_group_2_forwarders,
                    kafka_clusters, kafka_stanza):
    topic_group = _create_topic_group_set(kafka_stanza, kafka_clusters,
                                          topic_group_2_forwarders)
    if not topic_group:
        return _pick_forwarder_with_min_share(current_shares)

    # if in a topic group, try to assign topic in the same group to the same HF
    forwarders = topic_group_2_forwarders[kafka_stanza]
    if forwarders[topic_group]:
        shares = {}
        for f in forwarders[topic_group]:
            if f in current_shares:
                shares[f] = current_shares[f]
            else:
                shares[f] = 0
        minf = _pick_forwarder_with_min_share(shares)
    else:
        minf = _pick_forwarder_with_min_share(current_shares)
        forwarders[topic_group].add(minf)

    return minf


def _pick_forwarder_with_min_share(current_shares):
    minf = None
    min_share = sys.maxint
    for forwarder, share in current_shares.iteritems():
        if share < min_share:
            min_share = share
            minf = forwarder
    current_shares[minf] += 1
    return minf


def _create_topic_group_set(kafka_stanza, clusters, topic_group_2_forwarders):
    topic_group = clusters[kafka_stanza].get(c.kafka_topic_group)
    if topic_group and topic_group.strip():
        topic_group = topic_group.strip()
        if kafka_stanza not in topic_group_2_forwarders:
            topic_group_2_forwarders[kafka_stanza] = {}

        if topic_group not in topic_group_2_forwarders[kafka_stanza]:
            topic_group_2_forwarders[kafka_stanza][topic_group] = set()
    return topic_group


def _get_shares(clusters, forwarders, kafka_topics,
                dispatched_tasks, topic_group_2_forwarders):
    current_shares = {stanza: 0 for stanza in forwarders}
    for kafka_stanza, forwarder_tasks in dispatched_tasks.iteritems():
        if kafka_stanza not in kafka_topics:
            logger.warn("%s cluster is in ckpt file but not found in "
                        "kafka conf file", kafka_stanza)
            continue

        topic_group = _create_topic_group_set(
            kafka_stanza, clusters, topic_group_2_forwarders)
        for forwarder_stanza, forwarder_task in forwarder_tasks.iteritems():
            if forwarder_stanza not in forwarders:
                logger.warn("%s forwarder is in ckpt file but not found "
                            "in forwarder conf file", forwarder_stanza)
                continue

            for topic_partition in forwarder_task[c.kafka_topic]:
                topics = kafka_topics[kafka_stanza][c.kafka_topic]
                if topic_partition[0] not in topics:
                    logger.warn("Found deleted topic=%s on cluster=%s on "
                                "forwarder=%s", topic_partition[0],
                                kafka_stanza, forwarder_stanza)
                    continue

                current_shares[forwarder_stanza] += 1
                index = kafka_topics[kafka_stanza][c.index]
                if isinstance(topic_partition[-1], (str, unicode)):
                    if index == topic_partition[-1]:
                        topics.remove(topic_partition[0])
                    else:
                        logger.info("Detect index changed from %s to %s for "
                                    "%s", topic_partition[-1], index,
                                    kafka_stanza)

                if topic_group:
                    topic_group_2_forwarders[kafka_stanza][topic_group].add(
                        forwarder_stanza)
                logger.info("topic=%s on cluster=%s has already dispatched to "
                            "forwarder=%s", topic_partition[0], kafka_stanza,
                            forwarder_stanza)
    return current_shares


if __name__ == "__main__":
    task = {
        "kafka_brokers": "172.16.107.153:9092",
        "name": "LocalKC",
        "kafka_partition_offset": "earliest",
        "kafka_cluster": "LocalKC",
        "server_uri": "https://127.0.0.1:8089",
        "log_level": "INFO",
        "session_key": "tNc^Uo4aVmNkG8MbKnfjEmZ43IltNjK3AGunt40Eupn5AINcUm9KErJJCTtc8Z9JXZ0WCrUjAwTtKa0gCrsJCoaAo8JlLdw6fZ2CKoPwGwTfLnM4NwZ5BKoGPC",
        "use_kv_store": "0",
        "appName": "Splunk_TA_kafka",
        "userName": "nobody",
        "interval": "300",
        "host": "hydra",
        "checkpoint_dir": "/opt/splunk/var/lib/splunk/modinputs/kafka_mod",
        "kafka_topic": [
            [
                "small_records_topic",
                0,
                "earliest"
            ],
            [
                "small_records_topic",
                1,
                "earliest"
            ],
            [
                "small_records_topic",
                2,
                "earliest"
            ]
        ],
        "kafka_topic_group": "very_high",
        "kafka_topic_whitelist": "small_records_topic",
        "disabled": "0",
        "server_host": "hydra",
        "start_by_shell": "false",
        "index": "summary"
    }

    import copy
    import pprint

    cp_task = copy.deepcopy(task)
    cp_task["kafka_partition"] = "0,2,3",
    cp_task2 = copy.deepcopy(task)
    cp_task3 = copy.deepcopy(task)
    cp_task3[c.kafka_cluster] = "xxx"
    tasks = [task, task, cp_task, cp_task2, cp_task3]

    all_tasks = expand_or_merge_tasks(tasks, None)
    pprint.pprint(all_tasks)
    print len(all_tasks)
