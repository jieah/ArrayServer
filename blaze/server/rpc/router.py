import logging
log = logging.getLogger(__name__)
class RPCRouter(object):
    def route(self, unpacked):
        try:
            funcname = unpacked['msgobj']['func']
            args = unpacked['msgobj'].get('args', [])
            kwargs = unpacked['msgobj'].get('kwargs', {})
            func = getattr(self, "route_" + funcname)
            if len(unpacked['datastrs']) > 0:
                func(*args, unpacked=unpacked,
                     datastrs=unpacked['datastrs'],
                     **kwargs)
            else:
                func(*args, unpacked=unpacked,
                     **kwargs)


        except Exception as e:
            log.exception(e)

