"""
This module defines the core machinery that takes functions (or methods),
and wraps them as evaluation graph nodes.

This is used to build the .proxy module, which provides @graph_func-wrappped
functions for existing libraries like Numpy.
"""

DEBUG = False

class _Undefined(object):
    """ This singleton class is used as a placeholder value to indicate that
    a cached value has not yet been computed for a node.  We can't just use
    'None' because that may be a legitimate cached value.
    """
    pass
Undefined = _Undefined()

class GraphNode(object):
    """ Represents a node in the evaluation graph.  Primarily used to hold the
    function to be evaluated and its inputs.  All of its methods return
    GraphNode objects with the unbound methods as the callable and the previous
    GraphNode's output object as the 'self' parameter.
    """

    # If the number of listeners for a given node equals or exceeds this
    # number, then caching is turned on by default
    cache_threshold = 2

    # TODO: Eventually create a @graph_class class decorator which mixes
    # in the GraphNode class with any arbitrary other class (inserting
    # GraphNode as the first item in the MRO).

    def __init__(self, funcname, func, args=None, kwargs=None, flags=None, dispname=None):
        """
        Note that args and kwargs are a tuple and a dict, respectively,
        and are not *args and **kwargs.

        """
        self.funcname = funcname
        self.func = func
        self.args = args if args is not None else ()
        self.kwargs = kwargs if kwargs is not None else {}
        if flags is None:
            self.flags = ["cache"]
        else:
            self.flags = flags

        self.dispname = dispname if dispname is not None else ("%r"%self.func)
        self._value = Undefined
        self._listeners = []
        self._compiled_graph = None

    def eval(self, cache=None):
        """ If the cache parameter is explicitly provided on the eval() call,
        then it overrides the value of self.flags.
        """
        if cache is None:
            cache = "cache" in self.flags or len(self._listeners) > self.cache_threshold

        if cache and self._value is not Undefined:
            return self._value
        else:
            # TODO: handle tuples of arrays better
            if len(self.args) > 0 and isinstance(self.args[0], tuple):
                self.args[0] = tuple(arg if not isinstance(arg, GraphNode) else arg.eval(cache=False) for arg in self.args)
            else:
                args = [arg if not isinstance(arg, GraphNode) else arg.eval(cache=False) for arg in self.args]
            kwargs = dict((kv if not isinstance(kv[1], GraphNode) else (kv[0],kv[1].eval(cache=False))) \
                            for kv in self.kwargs.iteritems())
            #if DEBUG:
            #    print "EVAL: Node %s %d]" % (self.dispname, id(self))
            #    if self.funcname == "__getitem__":
            #        print "     getitem:", self.args
            val = self.func(*args, **kwargs)
            if cache:
                self._value = val
            else:
                self._value = Undefined
            return val

    def add_listener(self, listener):
        self._listeners.append(listener)

    #------------------------------------------------------------------------
    # Private methods
    #------------------------------------------------------------------------

    def invalidate(self):
        """ Invalidates our cached value """
        if "cache" in self.flags:
            if DEBUG:
                print "Invalidating", self
            self._value = Undefined
            for listener in self._listeners:
                listener.invalidate()

    #------------------------------------------------------------------------
    # Print and debug methods
    #------------------------------------------------------------------------

    def __str__(self):
        #if DEBUG:
        #    return "%r: func=%r, args=%r, kw=%r" % (self.__class__, self.func, self.args, self.kwargs)
        #else:
        return "%r: %r" % (self.__class__, self.func)
        #return "%s" % self.eval()

    def __repr__(self):
        #if DEBUG:
        #    return "%s: func=%r, args=%r, kw=%r" % (self.__class__, self.func, self.args, self.kwargs)
        #else:
        return "%r: %r" % (self.__class__, self.func)
        #return "%r" % self.eval()

    def dump(self, level=0, output=None):
        if output is None:
            import sys
            output = sys.stdout

        print >> output, " " * level*4, "Node", id(self), ":", self.func
        for arg in self.args:
            if isinstance(arg, GraphNode):
                arg.dump(level+1, output)
            else:
                print >> output, " "*(level+1)*4, type(arg), arg
        return

    def __getstate__(self):
        odict = self.__dict__.copy()
        del odict['_value']
        del odict['func']
        return odict

    def __setstate__(self, dict):
        self.__dict__.update(dict)
        self._value = Undefined


def cache(node):
    """ Turns on caching on a GraphNode and returns the node """
    node.flags.append("cache")
    return node


def graph_func(func):
    """ Wraps an input function so that instead of returning a concrete
    value, it returns a GraphNode, with our inner function as the func,
    and any arguments we were handed as the args and kwargs
    """

    return lambda *args, **kw: GraphNode(func, args, kw)


def find_nodes_of_type(graph, cls):
    """ Traverses the graph structure searching for nodes that are
    instances of the requested class :(object):
    """
    res = set()
    if isinstance(graph, cls): res.update([graph])
    for arg in graph.args:
        if isinstance(arg, cls): res.update([arg])
        if isinstance(arg, GraphNode): res.update(find_nodes_of_type(arg, cls))
    return res

#TODO: Implement on-graph predicates: IF..ELSE, WHILE, etc.


