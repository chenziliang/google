from googleapiclient import discovery
import oauth2client.client as oc

import splunktalib.rest as sr


def fqrn(resource_type, project, resource):
    """Return a fully qualified resource name for Cloud Pub/Sub."""
    return "projects/{}/{}/{}".format(project, resource_type, resource)


def create_google_client(config):
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
        "google_project": xxx,
        "google_subscription": xxx,
        "scopes": xxx,
        "service_name": xxx,
        "version": xxx,
    }
    """

    if config.get("google_credentials"):
        credentials = oc.get_application_credential_from_json(
            config["google_credentials"])
    else:
        credentials = oc.GoogleCredentials.get_application_default()

    if credentials.create_scoped_required():
        credentials = credentials.create_scoped(config["scopes"])

    http = sr.build_http_connection(
        config, timeout=config.get("pulling_interval", 30))
    client = discovery.build(
        config["service_name"], config["version"], http=http,
        credentials=credentials)
    return client
