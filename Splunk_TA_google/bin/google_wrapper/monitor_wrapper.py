import traceback

import google_wrapper.common as gwc


MONITOR_SCOPES = ["https://www.googleapis.com/auth/monitoring",
                  "https://www.googleapis.com/auth/cloud-platform"]


def get_pagination_results(service, req, key):
    all_results = []
    if req is None:
        return all_results

    result = req.execute(num_retries=3)
    if result and result.get(key):
        all_results.extend(result[key])

    if "nextPageToken" in result:
        while 1:
            req = service.list_next(req, result)
            if not req:
                break

            result = req.execute(num_retries=3)
            if result and result.get(key):
                all_results.extend(result[key])
            else:
                break

    return all_results


class GoogleCloudMonitor(object):

    def __init__(self, logger, config):
        """
        :param: config
        {
            "proxy_url": xxx,
            "proxy_port": xxx,
            "proxy_username": xxx,
            "proxy_password": xxx,
            "proxy_rdns": xxx,
            "proxy_type": xxx,
            "google_credentials": xxx,
        }
        """

        self._config = config
        self._config["scopes"] = MONITOR_SCOPES
        self._config["service_name"] = "cloudmonitoring"
        self._config["version"] = "v2beta2"
        self._logger = logger
        self._client = gwc.create_google_client(self._config)

    def list_metrics(self, params):
        """
        :params: dict like object
        {
        "google_project": xxx,
        "google_metric": xxx,
        "youngest": 2017-01-01T00:00:00-0000,
        "oldest": 2016-12-31T00:00:00-0000,
        ...
        }
        return:
        """

        try:
            timeseries = self._client.timeseries()
            req = timeseries.list(
                project=params["google_project"],
                oldest=params["oldest"], youngest=params["youngest"],
                metric=params["google_metric"], count=100)
            return get_pagination_results(timeseries, req, "timeseries")
        except Exception:
            self._logger.error(
                "Failed to list Google metric for project=%s, metric=%s, "
                "error=%s", params["google_project"],
                params["google_metric"], traceback.format_exc())
            raise

    def write_metrics(self, metrics):
        pass

    def metirc_descriptors(self, project_name):
        """
        return a list of metric_descriptor
        {
        "name": "appengine.googleapis.com/http/server/dos_intercept_count",
        "project": "1002621264351",
        "labels": [
            {
                 "key": "appengine.googleapis.com/module"
            },
            {
                 "key": "appengine.googleapis.com/version"
            },
            {
                 "key": "cloud.googleapis.com/location"
            },
            {
                 "key": "cloud.googleapis.com/service"
            }
        ],
        "typeDescriptor": {
            "metricType": "delta",
            "valueType": "int64"
        },
        "description": "Delta count of ... to prevent DoS attacks.",
        }
        """

        try:
            descriptors = self._client.metricDescriptors()
            req = descriptors.list(project=project_name)
            return get_pagination_results(descriptors, req, "metrics")
        except Exception:
            self._logger.error("Failed to list Google metric descriptors for "
                               "project=%s, error=%s",
                               project_name,  traceback.format_exc())
            raise


if __name__ == "__main__":
    import os
    import logging
    import pprint

    logger = logging.getLogger("google")
    ch = logging.StreamHandler()
    logger.addHandler(ch)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "zlchenken-78c88c5c115b.json"
    config = {
        "google_project": "zlchenken",
        "google_metric": "pubsub.googleapis.com/subscription/pull_request_count",
        "oldest": "2016-01-16T00:00:00-00:00",
        "youngest": "2016-02-16T00:00:00-00:00",
    }

    gcm = GoogleCloudMonitor(logger, config)
    descriptors = gcm.metirc_descriptors(config["google_project"])
    pprint.pprint(len(descriptors))
    pprint.pprint(descriptors)

    metrics = gcm.list_metrics(config)
    pprint.pprint(metrics)
