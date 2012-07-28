import logging
log = logging.getLogger(__name__)
class RPCRouter(object):
    def route(self, unpacked):
        try:
            funcname = unpacked['msgobj']['func']
            args = unpacked['msgobj'].get('args', [])
            kwargs = unpacked['msgobj'].get('kwargs', {})
            route_function = "route_" + funcname            
            if hasattr(self, route_function):
                func = getattr(self, route_function)
            else:
                func = self.default_route
            if len(unpacked['datastrs']) > 0:
                func(*args, unpacked=unpacked,
                     datastrs=unpacked['datastrs'],
                     **kwargs)
            else:
                func(*args, unpacked=unpacked,
                     **kwargs)
        except Exception as e:
            log.exception(e)

