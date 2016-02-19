"""
Data Loader main entry point
"""


import Queue
import os.path as op
import ConfigParser

import splunktalib.concurrent.concurrent_executor as ce
import splunktalib.timer_queue as tq
import splunktalib.schedule.job as sjob
import splunktalib.common.util as scutil
from splunktalib.common import log
from splunktalib import orphan_process_monitor as opm


class DataLoaderManager(object):
    """
    Data Loader boots all underlying facilities to handle data collection
    """

    def __init__(self, configs, job_scheduler, event_writer):
        """
        @configs: a list like object containing a list of dict
        like object. Each element shall implement dict.get/[] like interfaces
        to get the value for a key.
        @job_scheduler: schedulering the jobs. shall implement get_ready_jobs
        @event_writer: write_events
        """

        self._logger = log.Logs().get_logger(configs.get(
            "log_file", "util"))

        self._settings = self._read_default_settings(self._logger)
        self._configs = configs
        self._event_writer = event_writer
        self._wakeup_queue = Queue.Queue()
        self._scheduler = job_scheduler
        self._timer_queue = tq.TimerQueue()
        self._executor = ce.ConcurrentExecutor(self._settings)
        self._orphan_checker = opm.OrphanProcessChecker(None)
        self._started = False

    def start(self, jobs):
        if self._started:
            return
        self._started = True

        self._event_writer.start()
        self._executor.start()
        self._timer_queue.start()
        self._scheduler.start()
        self._logger.info("DataLoaderManager started.")

        for job in jobs:
            job.get_props()["event_writer"] = self._event_writer
            job.get_props()["data_loader_mgr"] = self

        def _enqueue_io_job(job):
            job_props = job.get_props()
            real_job = job_props["real_job"]
            self.run_io_jobs((real_job,))

        for job in jobs:
            j = sjob.Job(_enqueue_io_job, {"real_job": job},
                         job.get_interval())
            self._scheduler.add_jobs((j,))

        self._wait_for_tear_down()

        for job in jobs:
            job.stop()

        self._scheduler.tear_down()
        self._timer_queue.tear_down()
        self._executor.tear_down()
        self._event_writer.tear_down()
        self._logger.info("DataLoaderManager stopped.")

    def _wait_for_tear_down(self):
        wakeup_q = self._wakeup_queue
        while 1:
            try:
                go_exit = wakeup_q.get(timeout=1)
            except Queue.Empty:
                go_exit = self._orphan_checker.is_orphan()

            if go_exit:
                self._logger.info("DataLoaderManager got stop signal")
                self._started = False
                break

    def tear_down(self):
        self._wakeup_queue.put(True)
        self._logger.info("DataLoaderManager is going to stop.")

    def stopped(self):
        return not self._started

    def run_io_jobs(self, jobs, block=True):
        self._executor.enqueue_io_funcs(jobs, block)

    def run_compute_job(self, func, args=(), kwargs={}):
        self._executor.run_compute_func_sync(func, args, kwargs)

    def run_compute_job_async(self, func, args=(), kwargs={}, callback=None):
        """
        @return: AsyncResult
        """

        return self._executor.run_compute_func_async(func, args,
                                                     kwargs, callback)

    def add_timer(self, callback, when, interval):
        return self._timer_queue.add_timer(callback, when, interval)

    def remove_timer(self, timer):
        self._timer_queue.remove_timer(timer)

    def write_events(self, events):
        self._event_writer.write_events(events)

    def set_scheduler_randomization_limit(self, max_delay):
        assert max_delay > 0
        self._scheduler.max_delay_time = max_delay

    @staticmethod
    def _read_default_settings(logger):
        cur_dir = op.dirname(op.abspath(__file__))
        setting_file = op.join(cur_dir, "setting.conf")
        parser = ConfigParser.ConfigParser()
        parser.read(setting_file)
        settings = {}
        keys = ("process_size", "thread_min_size", "thread_max_size",
                "task_queue_size")
        for option in keys:
            try:
                settings[option] = parser.get("global", option)
            except ConfigParser.NoOptionError:
                settings[option] = -1

            try:
                settings[option] = int(settings[option])
            except ValueError:
                settings[option] = -1
        logger.debug("settings: %s", settings)
        return settings


def create_data_loader_mgr(configs):
    """
    create a data loader with default event_writer, job_scheudler
    """

    import splunktalib.event_writer as ew
    import splunktalib.schedule.scheduler as sched

    if scutil.is_true(configs.get("use_hec")):
        writer = ew.HecEventWriter(configs)
    elif scutil.is_true(configs.get("use_hec_raw")):
        writer = ew.RawHecEventWriter(configs)
    else:
        writer = ew.ModinputEventWriter()

    scheduler = sched.Scheduler()
    loader_mgr = DataLoaderManager(configs, scheduler, writer)
    return loader_mgr
