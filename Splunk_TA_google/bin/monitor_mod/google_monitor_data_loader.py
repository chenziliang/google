import traceback
import time
from json import dumps

from splunktalib.common import log
logger = log.Logs().get_logger("main")


import google_ta_common.google_consts as ggc
import monitor_mod.google_monitor_consts as gmc
import google_wrapper.monitor_wrapper as gmw


class GoogleMonitorMetricDataLoader(object):

    def __init__(self, config):
        """
        :config: dict object
        {
            "appname": xxx,
            "use_kv_store": xxx,
            "proxy_url": xxx,
            "proxy_port": xxx,
            "proxy_username": xxx,
            "proxy_password": xxx,
            "proxy_rdns": xxx,
            "proxy_type": xxx,
            "google_credentials": xxx,
            "google_project": xxx,
            "google_metric": xxx,
            "index": xxx,
        }
        """

        self._config = config
        self._source = "{project}:{metric}".format(
            project=self._config[gmc.google_project],
            subscription=self._config[gmc.google_subscription])
        self._running = False
        self._stopped = False

    def get_interval(self):
        return self._config[ggc.polling_interval]

    def stop(self):
        self._stopped = True
        logger.info("Stopping GooglePubSubDataLoader")

    def __call__(self):
        self.index_data()

    def index_data(self):
        if self._running:
            return
        self._running = True

        logger.info("Start indexing data for project=%s, subscription=%s",
                    self._config[gmc.google_project],
                    self._config[gmc.google_subscription])
        while not self._stopped:
            try:
                self._do_safe_index()
            except Exception:
                logger.error(
                    "Failed to index data for project=%s, subscription=%s, "
                    "error=%s", self._config[gmc.google_project],
                    self._config[gmc.google_subscription],
                    traceback.format_exc())
                time.sleep(2)
                continue
        logger.info("End of indexing data for project=%s, subscription=%s",
                    self._config[gmc.google_project],
                    self._config[gmc.google_subscription])

    def _do_safe_index(self):
        # 1) Cache max_events in memory before indexing for batch processing

        msgs_metrics = {
            "current_record_count": 0,
            "record_report_threshhold": 1000000,
            "record_report_start": time.time()
        }

        sub = gpw.GooglePubSub(logger, self._config)
        while not self._stopped:
            try:
                for msgs in sub.pull_messages():
                    if not self._stopped and msgs:
                        self._index_messages(msgs, msgs_metrics)
                        sub.ack_messages(msgs)
                    else:
                        break
            except Exception:
                logger.error(
                    "Failed to pull message from project=%s, subscription=%s, "
                    "error=%s", self._config[gmc.google_project],
                    self._config[gmc.google_subscription],
                    traceback.format_exc())
                time.sleep(2)
                continue
        self._running = False

    def _index_messages(self, msgs, msgs_metrics):
        msgs_metrics["current_record_count"] += len(msgs)
        current_count = msgs_metrics["current_record_count"]
        if current_count >= msgs_metrics["record_report_threshhold"]:
            logger.info(
                "index %s events for project=%s, subscription=%s takes "
                "time=%s", self._config[gmc.google_project],
                self._config[gmc.google_subscription], current_count,
                time.time() - msgs_metrics["record_report_start"])
            msgs_metrics["record_report_start"] = time.time()
            msgs_metrics["current_record_count"] = 0
        self._write_events(msgs)

    def _write_events(self, msgs):
        msgs_str = [dumps(msg["message"]) for msg in msgs]
        events = self._config[ggc.event_writer].create_events(
            index=self._config[ggc.index], host="", source=self._source,
            sourcetype="google:pubsub", time="", unbroken=False, done=False,
            events=msgs_str)
        self._config[ggc.event_writer].write_events(events)


if __name__ == "__main__":
    import sys
    import os
    import logging
    import threading

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
        ggc.data_loader: O(),
        ggc.event_writer: O(),
        ggc.checkpoint_dir: ".",
        ggc.server_uri: "https://localhost:8089",
        ggc.server_host: "localhost",
        ggc.index: "main",
        gmc.google_project: "zlchenken",
        gmc.google_topic: "test_topic",
        gmc.google_subscription: "sub_test_topic",
        gmc.batch_count: 10,
        gmc.base64encoded: True,
    }

    def pub():
        ps = gpw.GooglePubSub(logger, config)
        for i in range(10):
            messages = ["i am counting {} {}".format(i, j) for j in range(10)]
            ps.publish_messages(messages)
            time.sleep(1)

    pubthr = threading.Thread(target=pub)
    pubthr.start()

    loader = GooglePubSubDataLoader(config)

    subthr = threading.Thread(target=loader.index_data)
    subthr.start()

    pubthr.join()
    time.sleep(1)
    loader.stop()
    subthr.join()

#    import cProfile
#    import pstats
#    import cStringIO
#
#    pr = cProfile.Profile()
#    pr.enable()
#
#    pr.disable()
#    s = cStringIO.StringIO()
#    sortby = 'cumulative'
#    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
#    ps.print_stats()
#    print s.getvalue()
