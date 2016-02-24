import Queue
import multiprocessing
import threading
import sys
import json
import traceback
import time

import splunktalib.common.util as scutil
from splunktalib.common import log


import splunktalib.rest as sr

logger = log.Logs().get_logger("util")


scutil.disable_stdout_buffer()


class ModinputEvent(object):

    base_fmt = ("""<event{unbroken}>"""
                "<index>{index}</index>"
                "<host>{host}</host>"
                "<source>{source}</source>"
                "<sourcetype>{sourcetype}</sourcetype>"
                "<time>{time}</time>"
                "<data>{data}</data>{done}</event>")

    event_fmt = base_fmt.replace("{unbroken}", "").replace("{done}", "")
    unbroken_fmt = base_fmt.replace("{unbroken}", ' unbroken="1"').replace(
        "{done}", "")
    done_fmt = base_fmt.replace("{unbroken}", ' unbroken="1"').replace(
        "{done}", "<done/>")

    def __init__(self, index, host, source, sourcetype, time,
                 unbroken, done, events):
        self._string_events = self._format_events(
            index, host, source, sourcetype, time, events, unbroken, done)

    def _format_events(self, index, host, source, sourcetype, time,
                       events, unbroken, done):
        evt_fmt = self.event_fmt
        if done:
            evt_fmt = self.done_fmt
        elif unbroken:
            evt_fmt = self.unbroken_fmt

        if isinstance(events, (list, tuple)):
            res = "".join([self._do_format(
                evt, evt_fmt, index, host, source, sourcetype, time)
                for evt in events])
        elif isinstance(events, (str, unicode)):
            res = self._do_format(
                events, evt_fmt, index, host, source, sourcetype, time)
        else:
            assert 0

        return "<stream>{}</stream>".format(res)

    def _do_format(self, evt, evt_fmt, index, host, source, sourcetype, time):
        evt = scutil.escape_cdata(evt)
        res = evt_fmt.format(index=index, host=host, source=source,
                             sourcetype=sourcetype, time=time, data=evt)
        return res

    def to_string(self):
        return self._string_events


class ModinputEventWriter(object):

    def __init__(self, process_safe=False):
        if process_safe:
            self._mgr = multiprocessing.Manager()
            self._event_queue = self._mgr.Queue(1000)
        else:
            self._event_queue = Queue.Queue(1000)
        self._event_writer = threading.Thread(target=self._do_write_events)
        self._started = False

    def start(self):
        if self._started:
            return
        self._started = True

        self._event_writer.start()
        logger.info("ModinputEventWriter started.")

    def tear_down(self):
        if not self._started:
            return
        self._started = False

        self._event_queue.put(None)
        self._event_writer.join()
        logger.info("ModinputEventWriter stopped.")

    def write_events(self, events, retry=3):
        """
        :param evetns: list of ModinputEvent objects
        """

        if events is None:
            return

        self._event_queue.put(events)

    @staticmethod
    def create_events(index, host, source, sourcetype, time, unbroken,
                      done, events):
        return [ModinputEvent(index=index, host=host, source=source,
                              sourcetype=sourcetype, time=time,
                              unbroken=unbroken, done=done, events=events)]

    def _do_write_events(self):
        event_queue = self._event_queue
        write = sys.stdout.write
        got_shutdown_signal = False

        while 1:
            try:
                events = event_queue.get(timeout=3)
            except Queue.Empty:
                # We need drain the queue before shutdown
                # timeout means empty for now
                if got_shutdown_signal:
                    logger.info("ModinputEventWriter is going to exit...")
                    break
                else:
                    continue

            if events is not None:
                if isinstance(events, (str, unicode)):
                    # for legacy interface
                    write(events)
                else:
                    for event in events:
                        write(event.to_string())
            else:
                logger.info("ModinputEventWriter got tear down signal")
                got_shutdown_signal = True


class HecEventWriter(object):

    def __init__(self, config):
        """
        :params config: dict
        {
        "token": required,
        "hec_server_uri": required,
        "proxy_hostname": yyy,
        "proxy_url": zz,
        "proxy_port": aa,
        "proxy_username": bb,
        "proxy_password": cc,
        "proxy_type": http,http_no_tunnel,sock4,sock5,
        "proxy_rdns": 0 or 1,
        }
        """

        self._config = config
        self._http = sr.build_http_connection(
            config, disable_ssl_validation=True)
        self._compose_uri_headers(config)

    def _compose_uri_headers(self, config):
        self._uri = "{host}/services/collector".format(
            host=config["hec_server_uri"])
        self._headers = {
            "Authorization": "Splunk {}".format(config["token"]),
            "User-Agent": "curl/7.29.0",
            "Connection": "keep-alive",
        }

    def _prepare_events(self, events):
        """
        :param events: json dict list
        """

        return "\n".join(json.dumps(evt) for evt in events)

    def write_events(self, events, retry=3):
        """
        :params: events a list of json dict which meets HEC event schema
        {
        "event": xx,
        "index": yy,
        "host": yy,
        "source": yy,
        "sourcetype": yy,
        "time": yy,
        }
        Clients should consider batching, since when batching here, upper layer
        may have data loss
        """

        last_ex = None
        events = self._prepare_events(events)
        for _ in range(retry):
            try:
                response, content = self._http.request(
                    self._uri, method="POST", headers=self._headers,
                    body=events)
                if response.status in (200, 201):
                    return
                else:
                    msg = ("Failed to post events to HEC_URI={}, "
                           "error_code={}, reason={}").format(
                               self._uri, response.status, content)
                    logger.error(msg)
                    # We raise here to commonly use the below code block
                    raise Exception(msg)
            except Exception as e:
                last_ex = e
                logger.error("Failed to post events to HEC_URI=%s, error=%s",
                             self._uri, traceback.format_exc())
                self._http = sr.build_http_connection(
                    self._config, disable_ssl_validation=True)
                time.sleep(2)
        raise last_ex

    def start(self):
        pass

    def tear_down(self):
        pass

    @staticmethod
    def create_events(index, host, source, sourcetype,
                      time, unbroken, done, events):
        keys = [index, host, source, sourcetype, time]
        for i, key in enumerate(keys):
            if not key:
                keys[i] = None
        index, host, source, sourcetype, time = keys

        return [
            {
                "index": index,
                "host": host,
                "source": source,
                "sourcetype": sourcetype,
                "time": time,
                "event": event,
            } for event in events]


class RawHecEventWriter(HecEventWriter):

    def __init__(self, config):
        """
        :param: config should meet HecEventWriter param and include
        "channel"
        """

        super(RawHecEventWriter, self).__init__(config)

    def _compose_uri_headers(self, config):
        self._uri = "{host}/services/collector/raw".format(
            host=config["hec_server_uri"])
        self._headers = {
            "Authorization": "Splunk {}".format(config["token"]),
            "User-Agent": "curl/7.29.0",
            "Connection": "keep-alive",
            "x-splunk-request-channel": "{}".format(config["channel"]),
        }

    def _prepare_events(self, events):
        """
        :param events: string
        """

        # FIXME source, sourcetype etc

        return events


def create_event_writer(config, process_safe=False):
    if scutil.is_true(config.get("use_hec")):
        return HecEventWriter(config)
    elif scutil.is_true(config.get("use_raw_hec")):
        return RawHecEventWriter(config)
    else:
        return ModinputEventWriter(process_safe=process_safe)


if __name__ == "__main__":
    all_events = [["i love you"], ["1", "2", "3"]]

    index_events = []
    for events in all_events:
        for unbroken in (1, 0):
            for done in (0, 1):
                evt = ModinputEvent(
                    index="main", host="localhost", source="test",
                    sourcetype="test:json", time=time.time(),
                    unbroken=unbroken, done=done, events=events)
                index_events.append(evt)
                print evt.to_string()

    print "\n\n"
    writer = ModinputEventWriter()
    writer.start()
    writer.write_events(index_events)
    writer.tear_down()

    print "\n\n"
    writer = ModinputEventWriter(process_safe=True)
    writer.start()
    writer.write_events(index_events)
    writer.tear_down()

    import uuid
    config = {
        "hec_server_uri": "https://localhost:8088",
        "token": "139979A1-C8DD-42AF-9A8E-A2E91E7D0440",
        "channel": str(uuid.uuid4()),
    }

    writer = HecEventWriter(config)
    event = {
        "index": "main",
        "source": "hec_test",
        "sourcetype": "hec:test",
        "event": "i love you",
        "time": time.time(),
    }

    writer.write_events([event] * 10)
