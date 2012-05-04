import logging
log = logging.getLogger(__name__)
class RPCRouter(object):
    def route(self, msgobj, datastrs, rawmessage):
        try:
            funcname = msgobj['func']
            args = msgobj.get('args', [])
            kwargs = msgobj.get('kwargs', {})
            func = getattr(self, "route_" + funcname)
            if len(datastrs) > 0:
                kwargs['datastrs'] = datastrs
            kwargs['rawmessage'] = rawmessage
            func(*args, **kwargs)
        except Exception as e:
            log.exception(e)

