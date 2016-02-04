import threading
import Queue
from multiprocessing import Manager
from multiprocessing import Process
import os

from splunktalib.common import log
logger = log.Logs().get_logger("main")

import splunktalib.event_writer as ew
import splunktalib.timer_queue as tq
import splunktalib.common.util as scutil
import splunktalib.orphan_process_monitor as opm

import pubsub_mod.google_pubsub_data_loader as gdl
import pubsub_mod.google_pubsub_consts as gpc
import google_ta_common.google_consts as ggc


def create_data_loader(config):
    service_2_data_loader = {
        ggc.google_pubsub: gdl.GooglePubSubDataLoader,
    }

    assert config.get(ggc.google_service)
    assert config[ggc.google_service] in service_2_data_loader

    return service_2_data_loader[config[ggc.google_service]](config)


def create_event_writer(config, process_safe):
    if scutil.is_true(config.get(ggc.use_hec)):
        return ew.HecEventWriter(config)
    elif scutil.is_true(config.get(ggc.use_raw_hec)):
        return ew.RawHecEventWriter(config)
    else:
        return ew.ModinputEventWriter(process_safe=process_safe)


def _wait_for_tear_down(tear_down_q, loader):
    checker = opm.OrphanProcessChecker()
    while 1:
        try:
            go_exit = tear_down_q.get(block=True, timeout=2)
        except Queue.Empty:
            go_exit = checker.is_orphan()
            if go_exit:
                logger.info("%s becomes orphan, going to exit", os.getpid())

        if go_exit:
            break

    if loader is not None:
        loader.stop()
    logger.info("End of waiting for tear down signal")


def _load_data(tear_down_q, task_config):
    loader = create_data_loader(task_config)
    thr = threading.Thread(
        target=_wait_for_tear_down, args=(tear_down_q, loader))
    thr.daemon = True
    thr.start()
    loader.index_data()
    thr.join()
    logger.info("End of load data")


class GoogleConcurrentDataLoader(object):

    def __init__(self, task_config, tear_down_q, process_safe):
        if process_safe:
            self._worker = Process(
                target=_load_data, args=(tear_down_q, task_config))
        else:
            self._worker = threading.Thread(
                target=_load_data, args=(tear_down_q, task_config))

        self._worker.daemon = True
        self._started = False
        self._tear_down_q = tear_down_q
        self.name = task_config[ggc.name]

    def start(self):
        if self._started:
            return
        self._started = True

        self._worker.start()
        logger.info("GoogleConcurrentDataLoader started.")

    def tear_down(self):
        self.stop()

    def stop(self):
        if not self._started:
            return
        self._started = False

        self._tear_down_q.put(True)
        logger.info("GoogleConcurrentDataLoader is going to exit.")


class GoogleDataLoaderManager(object):

    def __init__(self, task_configs):
        self._task_configs = task_configs
        self._wakeup_queue = Queue.Queue()
        self._timer_queue = tq.TimerQueue()
        self._started = False
        self._stop_signaled = False

    def start(self):
        if self._started:
            return
        self._started = True

        self._timer_queue.start()

        process_safe = self._use_multiprocess()
        logger.info("Use multiprocessing=%s", process_safe)

        event_writer = create_event_writer(self._task_configs[0], process_safe)
        event_writer.start()

        tear_down_q = self._create_tear_down_queue(process_safe)

        loaders = []
        for task in self._task_configs:
            task[ggc.event_writer] = event_writer
            loader = GoogleConcurrentDataLoader(
                task, tear_down_q, process_safe)
            loader.start()
            loaders.append(loader)

        logger.info("GoogleDataLoaderManager started")
        _wait_for_tear_down(self._wakeup_queue, None)
        logger.info("GoogleDataLoaderManager got stop signal")

        for loader in loaders:
            logger.info("Notify loader=%s", loader.name)
            loader.stop()

        event_writer.tear_down()
        self._timer_queue.tear_down()

        logger.info("GoogleDataLoaderManager stopped")

    def tear_down(self):
        self.stop()

    def stop(self):
        self._stop_signaled = True
        self._wakeup_queue.put(True)
        logger.info("GoogleDataLoaderManager is going to stop.")

    def stopped(self):
        return not self._started

    def received_stop_signal(self):
        return self._stop_signaled

    def add_timer(self, callback, when, interval):
        return self._timer_queue.add_timer(callback, when, interval)

    def remove_timer(self, timer):
        self._timer_queue.remove_timer(timer)

    def _use_multiprocess(self):
        if not self._task_configs:
            return False

        return self._task_configs[0].get(ggc.use_multiprocess)

    def _create_tear_down_queue(self, process_safe):
        if process_safe:
            self._mgr = Manager()
            tear_down_q = self._mgr.Queue()
        else:
            tear_down_q = Queue.Queue()
        return tear_down_q


if __name__ == "__main__":
    import time
    import sys
    import logging
    import google_wrapper.pubsub_wrapper as gpw

    class O(object):
        def write_events(self, index, source, sourcetype, events):
            for event in events:
                sys.stdout.write(event)
                sys.stdout.write("\n")

    logger = logging.getLogger("google")
    ch = logging.StreamHandler()
    logger.addHandler(ch)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "zlchenken-78c88c5c115b.json"
    config = {
        ggc.name: "test_pubsub",
        ggc.google_service: ggc.google_pubsub,
        ggc.checkpoint_dir: ".",
        ggc.server_uri: "https://localhost:8089",
        ggc.server_host: "localhost",
        ggc.index: "main",
        gpc.google_project: "zlchenken",
        gpc.google_topic: "test_topic",
        gpc.google_subscription: "sub_test_topic",
        gpc.batch_count: 10,
        gpc.base64encoded: True,
    }

    def pub():
        ps = gpw.GooglePubSub(logger, config)
        for i in range(10):
            messages = ["i am counting {} {}".format(i, j) for j in range(10)]
            ps.publish_messages(messages)
            time.sleep(1)

    pubthr = threading.Thread(target=pub)
    pubthr.start()

    l = GoogleDataLoaderManager([config])

    def _tear_down():
        time.sleep(30)
        l.stop()

    threading.Thread(target=_tear_down).start()
    l.start()
    pubthr.join()
    time.sleep(1)
