import httplib
import json
import webob

from qonos.openstack.common import wsgi


class Controller(object):
    def index(self, req):
        """Respond to a request for all OpenStack API versions."""
        def build_version_object(version, path, status):
            return {
                'id': 'v%s' % version,
                'status': status,
                'links':
                [
                    {
                        'rel': 'self',
                        'href': '%s/%s/' % (req.host_url, path),
                    },
                ],
            }
        version_objs = []
        version_objs.append(build_version_object(1.0, 'v1', 'CURRENT'))

        response = webob.Response(request=req, status=httplib.MULTIPLE_CHOICES,
                                  content_type='application/json')
        response.body = json.dumps(dict(versions=version_objs))
        return response

    @webob.dec.wsgify(RequestClass=wsgi.Request)
    def __call__(self, req):
        return self.index(req)


def create_resource(conf):
    return wsgi.Resource(Controller())
