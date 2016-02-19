import sys
import datetime
import time
import os.path as op

import splunktalib.common.util as scutil
import splunktalib.modinput as mi
from splunktalib.common import log
import splunktalib.file_monitor as fm

import google_ta_common.google_consts as ggc


scutil.disable_stdout_buffer()


def validate_config():
    """
    Validate inputs.conf
    """

    return 0


def usage():
    """
    Print usage of this binary
    """

    hlp = "%s --scheme|--validate-arguments|-h"
    print >> sys.stderr, hlp % sys.argv[0]
    sys.exit(1)


def print_scheme(title, description):
    """
    Feed splunkd the TA's scheme
    """

    print """
    <scheme>
    <title>{title}</title>
    <description>{description}</description>
    <use_external_validation>true</use_external_validation>
    <streaming_mode>xml</streaming_mode>
    <use_single_instance>true</use_single_instance>
    <endpoint>
      <args>
        <arg name="name">
          <title>Unique name which identifies this data input</title>
        </arg>
        <arg name="placeholder">
          <title>placeholder</title>
        </arg>
      </args>
    </endpoint>
    </scheme>""".format(title=title, description=description)


def main(scheme_printer, run):
    args = sys.argv
    if len(args) > 1:
        if args[1] == "--scheme":
            scheme_printer()
        elif args[1] == "--validate-arguments":
            sys.exit(validate_config())
        elif args[1] in ("-h", "--h", "--help"):
            usage()
        else:
            usage()
    else:
        run()


def setup_signal_handler(loader, logger):
    """
    Setup signal handlers
    @data_loader: data_loader.DataLoader instance
    """

    def _handle_exit(signum, frame):
        logger.info("Exit signal received, exiting...")
        loader.tear_down()

    scutil.handle_tear_down_signals(_handle_exit)


def get_file_change_handler(loader, logger):
    def reload_and_exit(changed_files):
        logger.info("Conf file(s)=%s changed, exiting...", changed_files)
        loader.tear_down()

    return reload_and_exit


def get_configs(ConfCls, modinput_name, logger):
    conf = ConfCls()
    tasks = conf.get_tasks()

    if tasks:
        loglevel = tasks[0].get(ggc.log_level, "INFO")
        log.Logs().set_level(loglevel)
    else:
        logger.info(
            "Data collection for %s is not fully configured. "
            "Doing nothing and quiting the TA.", modinput_name)
        return None, None

    return conf.metas, tasks


def parse_datetime(splunk_uri, session_key, time_str):
    """
    Leverage splunkd to do time parseing,
    :time_str: ISO8601 format, 2011-07-06T21:54:23.000-07:00
    """

    import splunklib

    if not time_str:
        return None

    scheme, host, port = tuple(splunk_uri.replace("/", "").split(":"))

    service = splunklib.client.Service(token=session_key, scheme=scheme,
                                       host=host, port=port)
    endpoint = splunklib.client.Endpoint(service, "search/timeparser/")
    r = endpoint.get(time=time_str, output_time_format="%s")
    response = splunklib.data.load(r.body.read()).response
    seconds = response[time_str]
    return datetime.datetime.utcfromtimestamp(float(seconds))


def get_modinput_configs():
    modinput = sys.stdin.read()
    return mi.parse_modinput_configs(modinput)


def sleep_until(interval, condition):
    """
    :interval: integer
    :condition: callable to check if need break the sleep loop
    :return: True when during sleeping condition is met, otherwise False
    """

    for _ in range(interval):
        time.sleep(1)
        if condition():
            return True
    return False


def get_conf_files(files):
    cur_dir = op.dirname(op.dirname(op.abspath(__file__)))
    files = []
    all_confs = [ggc.myta_global_settings_conf, ggc.myta_cred_conf] + files
    for f in all_confs:
        files.append(op.join(cur_dir, "local", f))
    return files


def create_conf_monitor(callback, files):
    return fm.FileMonitor(callback, get_conf_files(files))
