import traceback
from json import dumps
import threading

from splunktalib.common import log
logger = log.Logs().get_logger("main")


import google_ta_common.google_consts as ggc
import google_wrapper.cloud_monitor_wrapper as gmw
import cloud_monitor_mod.google_cloud_monitor_consts as gmc


class GoogleCloudMonitorDataLoader(object):

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
            project=self._config[ggc.google_project],
            metric=self._config[gmc.google_metric])
        self._lock = threading.Lock()
        self._stopped = False

    def get_interval(self):
        return int(self._config[ggc.polling_interval])

    def get_props(self):
        return self._config

    def stop(self):
        self._stopped = True
        logger.info("Stopping GooglePubSubDataLoader")

    def __call__(self):
        self.index_data()

    def index_data(self):
        if self._lock.locked():
            logger.info("Last time data collect for project=%s, metric=%s is "
                        "not done", self._config[ggc.google_project],
                        self._config[gmc.google_metric])
            return

        with self._lock:
            self._do_index()

    def _do_index(self):
        logger.info("Start collecting data for project=%s, metric=%s",
                    self._config[ggc.google_project],
                    self._config[gmc.google_metric])
        try:
            self._do_safe_index()
        except Exception:
            logger.error(
                "Failed to collect data for project=%s, metric=%s, error=%s",
                self._config[ggc.google_project],
                self._config[gmc.google_metric], traceback.format_exc())
        logger.info("End of collecting data for project=%s, metric=%s",
                    self._config[ggc.google_project],
                    self._config[gmc.google_metric])

    def _do_safe_index(self):
        # 1) Cache max_events in memory before indexing for batch processing

        params = {
            ggc.google_project: self._config[ggc.google_project],
            gmc.google_metric: self._config[gmc.google_metric],
            gmc.youngest: "2016-02-18T00:00:00-00:00",
            gmc.oldest: "2016-01-01T00:00:00-00:00",
        }

        # FIXME ckpt, time win
        mon = gmw.GoogleCloudMonitor(logger, self._config)
        metrics = mon.list_metrics(params)
        if metrics:
            self._write_events(metrics)

    def _write_events(self, metrics):
        msgs_str = [dumps(metric) for metric in metrics]
        events = self._config[ggc.event_writer].create_events(
            index=self._config[ggc.index], host="", source=self._source,
            sourcetype="google:pubsub", time="", unbroken=False, done=False,
            events=msgs_str)
        self._config[ggc.event_writer].write_events(events)


if __name__ == "__main__":
    import os
    import splunktalib.event_writer as ew

    writer = ew.ModinputEventWriter()
    writer.start()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "zlchenken-78c88c5c115b.json"
    config = {
        ggc.data_loader: writer,
        ggc.event_writer: writer,
        ggc.checkpoint_dir: ".",
        ggc.server_uri: "https://localhost:8089",
        ggc.server_host: "localhost",
        ggc.index: "main",
        ggc.google_project: "zlchenken",
        gmc.google_metric: "pubsub.googleapis.com/subscription/pull_request_count",
    }

    loader = GoogleCloudMonitorDataLoader(config)
    loader.index_data()
    writer.tear_down()