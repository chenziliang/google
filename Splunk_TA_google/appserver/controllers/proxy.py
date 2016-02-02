import logging
import math
import re
import json
import urllib

import cherrypy
import splunk

import xml.sax.saxutils as su
import splunk.appserver.mrsparkle.controllers as controllers
import splunk.appserver.mrsparkle.lib.util as util
from splunk.appserver.mrsparkle.lib.util import make_url


from splunk.appserver.mrsparkle.lib.decorators import expose_page
from splunk.appserver.mrsparkle.lib.routes import route

logger = logging.getLogger("splunk")
logger.setLevel(logging.INFO)


_PROXY_WHITE_LIST = [
    {"endpoint": "splunk_ta_google/google_projects", "methods": ["GET"]},
    {"endpoint": "splunk_ta_google/google_subscriptions", "methods": ["GET"]},
]


def precompile_whitelist():
    for props in _PROXY_WHITE_LIST:
        regex_string = "(^|^services/|^servicesNS/[^/]+/[^/]+/)%s$" % props["endpoint"]
        regex = re.compile(regex_string)
        props["endpoint"] = regex

precompile_whitelist()


class GoogleProxyManager(controllers.BaseController):

    @route("*")
    @expose_page(must_login=True, methods=["GET", "POST", "PUT", "DELETE"])
    def applyProxy(self, **args):
        if (cherrypy.request.method in ["POST", "DELETE"] and
                not cherrypy.config.get("enable_proxy_write")):
            return self.generateError(
                405, _("Write access to the proxy endpoint is disabled."))

        sessionKey = cherrypy.session.get("sessionKey")
        cherrypy.session.release_lock()

        # We have to handle the fact that CherryPy is going to %-decode
        # the URL, including any "/" (%2F). As such, we use the relative_uri
        # (which doesn"t %-decode %2F), and simply re-encode that URL
        logger.debug("[Proxy Traffic] %s request to: %s",
                     cherrypy.request.method, cherrypy.request.relative_uri)
        relative_uri = cherrypy.request.relative_uri
        relative_uri = relative_uri[relative_uri.find("/proxy") + 7:]
        query_start = relative_uri.rfind("?")
        if (query_start > -1) and (cherrypy.request.query_string):
            relative_uri = relative_uri[:query_start]

        uri = urllib.quote(relative_uri)
        rawResult = True

        try:
            endpointProps = self.getAllowedEndpointProps(
                uri, cherrypy.request.method)
            if endpointProps is None:
                # endpoint not allowed
                logger.info("HELP Resource not found: %s" % uri)
                raise cherrypy.HTTPError(404, _("Resource not found: %s" % uri))
        except Exception, e:
            logger.exception(e)
            return self.generateError(500, su.escape(str(e)))

        # CSRF Protection
        requireValidFormKey = not endpointProps.get(
            "skipCSRFProtection", False)
        if not util.checkRequestForValidFormKey(requireValidFormKey):
            # checkRequestForValidFormKey() will raise an error if the
            # request was an xhr, but we have to handle if not-xhr
            raise cherrypy.HTTPError(
                401, _("Splunk cannot authenticate the request. CSRF validation failed."))

        # Force URI to be relative so an attacker can"t hit any arbitrary URL
        uri = "/" + uri

        if cherrypy.request.query_string:
            queryArgs = cherrypy.request.query_string.split("&")
            # need to remove the browser cache-busting _=XYZ that is
            # inserted by cache:false (SPL-71743)
            modQueryArgs = [queryArg for queryArg in queryArgs
                            if not queryArg.startswith("_=")]
            uri += "?" + "&".join(modQueryArgs)

        logger.debug("Proxying: %s" % uri)

        postargs = None
        body = None
        if cherrypy.request.method in ("POST", "PUT"):
            content_type = cherrypy.request.headers.get("Content-Type", "")
            if not content_type or content_type.find("application/x-www-form-urlencoded") > -1:
                # We use the body_params to avoid mixing up GET/POST arguments,
                # which is the norm with output_mode=json in Ace.
                logger.debug("[Splunkweb Proxy Traffic] request body: %s",
                             cherrypy.request.body_params)
                postargs = cherrypy.request.body_params
            else:
                # special handing for application/json POST
                # cherrypy gives file descriptor for POST"s
                body = cherrypy.request.body.read()
                logger.debug(
                    "[Splunkweb Proxy Traffic] request body: %s", body)

        proxyMode = False
        if "authtoken" in args:
            proxyMode = True

        simpleRequestTimeout = splunk.rest.SPLUNKD_CONNECTION_TIMEOUT
        if "timeout" in endpointProps:
                simpleRequestTimeout = endpointProps["timeout"]

        try:
            logger.debug("Proxying: %s" % uri)
            serverResponse, serverContent = splunk.rest.simpleRequest(
                make_url(uri, translate=False, relative=True, encode=False),
                sessionKey,
                postargs=postargs,
                method=cherrypy.request.method,
                raiseAllErrors=True,
                proxyMode=proxyMode,
                rawResult=rawResult,
                jsonargs=body,
                timeout=simpleRequestTimeout
            )

            for header in serverResponse:
                cherrypy.response.headers[header] = serverResponse[header]

            # respect presence of content-type header
            if serverResponse.get("content-type") is None:
                del cherrypy.response.headers["Content-Type"]

            logger.debug("[Splunkweb Proxy Traffic] response status code: %s",
                         serverResponse.status)

            if serverResponse.messages:
                return self.generateError(
                    serverResponse.status, serverResponse.messages)

            if rawResult:
                cherrypy.response.status = serverResponse.status

            logger.debug("[Splunkweb Proxy Traffic] response body: %s",
                         serverContent)
            return serverContent
        except splunk.RESTException, e:
            logger.exception(e)
            return self.generateError(e.statusCode, e.extendedMessages)
        except Exception, e:
            logger.exception(e)
            return self.generateError(500, su.escape(str(e)))

    def getAllowedEndpointProps(self, uri, method):
        """verify that that a given uri and associated method is white
        listed to be proxied to the endpoint."""

        for props in _PROXY_WHITE_LIST:
            if props["endpoint"].match(uri):
                if method in props["methods"]:
                    return props
        else:
            return None

    def generateError(self, status, messages=None):
        def generateErrorJson():
            cherrypy.response.headers["Content-Type"] = "application/json"
            output = {}
            output["status"] = su.escape(str(status))
            if messages:
                if isinstance(messages, list):
                    escaped_messages = [{
                        "type": su.escape(msg["type"]),
                        "text":su.escape(msg["text"])
                    } for msg in messages]
                    output["messages"] = escaped_messages
                else:
                    msg = {"type": "ERROR", "text": su.escape(messages)}
                    output["messages"] = [msg]
            return json.dumps(output)

        def generateErrorXml():
            output = [splunk.rest.format.XML_MANIFEST, "<response>"]
            output.append('<meta http-equiv="status" content="%s" />'
                          % su.escape(str(status)))
            if messages:
                output.append("<messages>")

                if isinstance(messages, list):
                    for msg in messages:
                        output.append(
                            '<msg type="%s">%s</msg>'
                            % (su.escape(msg["type"]), su.escape(msg["text"])))
                else:
                    output.append('<msg type="ERROR">%s</msg>' % str(messages))
                output.append("</messages>")

            output.append("</response>")
            return "\n".join(output)

        logger.debug("[Splunkweb Proxy Traffic] response errors: %s", messages)
        output_mode = cherrypy.request.params.get("output_mode")
        # make sure that error status is relayed back to client via status
        # code, and not just content
        cherrypy.response.status = status
        if output_mode and output_mode == "json":
            return generateErrorJson()
        return generateErrorXml()

    def validate_list_kwargs(self, kwargs):
        """ ensures that count and offset are safe and sane """

        count = 100
        offset = 0
        try:
            count = min(int(math.fabs(int(kwargs.get("count", 100)))), 100)
        except:
            pass

        try:
            offset = min(int(math.fabs(int(kwargs.get("offset", 0)))), 10)
        except:
            pass

        return count, offset
