import numbers
import numpy as np
from grapheval import GraphNode
import grapheval
client = None

class MetaArrayProxy(type):
    py_binary_graph_methods = (
        "reversed,contains,"
        "lt,le,eq,ne,gt,ge,cmp,"
        "add,sub,mul,div,mod,pow,floordiv,truediv,divmod,"
        "radd,rsub,rmul,rdiv,rmod,rpow,rfloordiv,rtruediv,rdivmod,"
        "iadd,isub,imul,idiv,imod,ipow,ifloordiv,itruediv,"
        "and,or,xor,lshift,rshift,"
        "rand,ror,rxor,rlshift,rrshift,"
        "iand,ior,ixor,ilshift,irshift"
    ).split(",")
    py_binary_graph_methods = ["__%s__"%n for n in py_binary_graph_methods]

    py_unary_graph_methods = (
        "abs,invert,neg,pos"
    ).split(",")
    py_unary_graph_methods = ["__%s__"%n for n in py_unary_graph_methods]

    pymethods = ("len,setitem,iter,complex,int,long,float,oct,hex,coerce,nonzero").split(",")
    pymethods = ["__%s__"%n for n in pymethods]

    # Array graph methods are ones that return arrays, and therefore need to be
    # wrapped to return ArrayProxies.
    #
    # Scalar graph methods are content-dependent methods that return scalars
    # from the array.  They are also wrapped to return GraphNodes, because
    # their evaluation needs data that can only be known afte a graph evaluation.
    #
    # Methods that return non-shape-dependent attributes of the array (e.g.
    # dtype,etc.) and modify it in-place are not graph methods, and return
    # immediate values.  If the user actually does want to regard them as graph
    # methods, they should be called with graph_call(proxy.method, *args,
    # **kw).  This will cause them to return a GraphNode that evaluates to a
    # scalar or None.

    npgraphmethods = ("all,any,argmax,argmin,argsort,astype,byteswap,"
        "choose,clip,compress,conj,conjugate,copy,cumprod,cumsum,"
        "diagonal,flatten,getfield,max,mean,min,newbyteorder,"
        "nonzero,prod,ptp,ravel,repeat,reshape,resize,round,"
        "searchsorted,sort,squeeze,std,"
        "sum,swapaxes,take,trace,transpose,var,view"
        # scalar methods
        "nbytes,item").split(",")

    npmethods = ("fill,flags,itemset,itemsize,put,setflags,"
        "setfield,strides,tofile,tolist,tostring").split(",")

    # This is a dict of all the overridden methods of an ndarray.  It is
    # created once, upon definition of this metaclass, and then subsequent
    # instantiations of classes using this metaclass will be a fast dict
    # update.
    # We use exec below because using a for-loop to declare the wrapped
    # functions is much more concise, but because of scoping, we can't access
    # the local "name" variable inside the lambda.  I can't figure out a way to
    # curry it into there, either, because on Python 2.x, the use of **kw
    # prevents us from declaring additional, keyword-only arguments.
    #
    # TODO: It's generally not appropriate to access obj._array in the
    # following lambdas, because an ArrayNode does not have an _array.  This
    # gets to the heart of the definition of "eval()" in the context of
    # generator arrays.  Especially any of the __i*__ math functions - don't
    # they need to have something to inncrement in place?  Or do we punt for
    # the time being an dprohibit any setting or overriding of elements in an
    # ArrayNode?

    wrapped_methods = {}
    for name in npgraphmethods:
        exec "wrapped_methods['%s'] = lambda obj,*a,**kw: obj._graph_call('%s',a,kw)" % (name,name)

    for name in py_unary_graph_methods:
        exec "wrapped_methods['%s'] = lambda obj: ArrayNode('%s', getattr(np.ndarray,'%s'), (obj,))" % (name, name, name)

    for name in py_binary_graph_methods:
        exec "wrapped_methods['%s'] = lambda obj, a: ArrayNode('%s', getattr(np.ndarray,'%s'), (obj, a))" % (name, name, name)

    for name in pymethods + npmethods:
        exec "wrapped_methods['%s'] = lambda obj,*a,**kw: obj._call('%s',a,kw)" % (name,name)

    # Numpy also features data attributes that return arrays, and these
    # need to be overriden on the instance to return ArrayProxies
    npdataattrs = ("dtype,data,flat,imag,real,T").split(",")
    npshapeattrs = ("ndim,shape,size").split(",")

    def __new__(cls, class_name, bases, class_dict):
        class_dict.update(cls.wrapped_methods)
        return type.__new__(cls, class_name, bases, class_dict)

class BaseArrayNode(GraphNode):

    __metaclass__ = MetaArrayProxy

    #------------------------------------------------------------------------
    # Abstract base methods
    #------------------------------------------------------------------------

    def _graph_call(self, funcname, args, kw):
        """ Indicates that a call to the given on-graph funcname has been made,
        and gives the subclass a chance to do whatever they want.  This method
        should, in general, return the same arguments or values that the
        underlying call produces.
        """
        raise NotImplementedError

    def _call(self, funcname, args, kw):
        """ Intervenes in the dispatch of a non-graph method or numpy method.
        In general, this should return a concrete value.  For generators, this
        usually results in an evaluation.
        """
        raise NotImplementedError

    def _get_np_attr(self, attrname):
        """ Some Numpy array attributes return views of transformations of the
        data, and those are routed through this method.
        """
        raise NotImplementedError

    #------------------------------------------------------------------------
    # Array methods that need special processing
    #------------------------------------------------------------------------

    def __getitem__(self, index):
        """ Slicing operations should return graph nodes, while individual
        element access should return bare scalars.
        """
        if isinstance(index, numbers.Integral) or isinstance(index, np.integer):
            return self._call("__getitem__", (index,), {})
        else:
            # slice objects, fancy indexing using tuples, and masking using
            # other arrays will return a graph node
            return self._graph_call("__getitem__", (index,), {})

    #------------------------------------------------------------------------
    # Private helper methods.  Subclasses should not have to worry about these.
    #------------------------------------------------------------------------

    def __getattribute__(self, name):
        if name in MetaArrayProxy.npdataattrs:
            return self._get_np_attr(name)
        elif name in MetaArrayProxy.npshapeattrs:
            # If the instance declares a specially-named getter method for
            # one of the shape-related attributes, then call it; otherwise,
            # call up to its first input.
            # TODO: Do this right, i.e. take slices and striding into account
            # and do all that index arithmetic stuff.
            if hasattr(self, "_"+name):
                return getattr(self, "_"+name)()
            else:
                # TODO: Improve how we find input arrays
                if len(self.args) > 0 and isinstance(self.args[0], tuple):
                    return self.args[0][0].ndim
                else:
                    arrayargs = tuple(arg for arg in self.args if isinstance(arg,np.ndarray) or isinstance(arg, BaseArrayNode))
                    if len(arrayargs) > 0:
                        return arrayargs[0].ndim
                    else:
                        import pdb; pdb.set_trace()

        else:
            return object.__getattribute__(self, name)

    def dump(self, level=0, output=None, prefix=""):
        if output is None:
            import sys
            output = sys.stdout
        prefix = " "*level*4 + prefix
        msg = \
"""%(prefix)s Node %(funcname)s (%(id)d) : %(dispname)s
%(prefixpad)s    self.args: %(sargs)r
%(prefixpad)s      self.kw: %(skwargs)r
""" % dict(prefix=prefix, id=id(self), funcname=self.funcname, dispname=self.dispname,
        sargs=self.args, skwargs=self.kwargs, prefixpad=" "*len(prefix))
        print >> output, msg


class ArrayProxy(BaseArrayNode):
    """ A Python-level class that wraps an ndarray instance.  All the methods
    of an ndarray are present here, and most will return an ArrayNode.

    The ArrayProxy presents a generator array interface that sits on top of an
    actual numpy array.
    """

    def __init__(self, ary):
        """ Creates a new ArrayProxy, optionally given an array as a parent
        value, or an expression graph, or evaluation flags.
        """
        super(ArrayProxy,self).__init__("", None)
        self._array = ary

    def set_array(self, ary):
        self._array = ary
        self.invalidate()

    def _ndim(self):
        return self._array.ndim

    def _shape(self):
        return self._array.shape

    def _size(self):
        return self._array.size

    def __setstate__(self, dict):
        super(ArrayProxy, self).__setstate__(dict)
        self.func = None

    #------------------------------------------------------------------------
    # GraphNode interface
    #------------------------------------------------------------------------

    def eval(self, cache=None):
        return self._array

    def _graph_call(self, funcname, args, kw):
        node = ArrayNode(funcname, self._call, (funcname, args, kw))
        self.add_listener(node)
        return node

    def _call(self, funcname, args, kw):
        # Need to walk the list of inputs and look for Nodes, and eval them.
        if grapheval.DEBUG:
            print "[%d] %s: _call(%s, %r, %r)" % (id(self), self.dispname, funcname, args, kw)
            #self.dump(prefix="CALL:")
            #print "         args:", args
            #print "           kw:", kw
        args = [arg if not isinstance(arg, GraphNode) else arg.eval() \
                for arg in args]
        kw = dict((k if not isinstance(k[1], GraphNode) else (k[0],k[1].eval())) for k in kw.iteritems())
        return getattr(self._array, funcname)(*args, **kw)

    def _get_np_attr(self, attrname):
        node = ArrayNode("." + attrname, self._deferred_getattr, attrname)
        self.add_listener(node)
        return node

    def _deferred_getattr(self, attrname):
        return getattr(self._array, attrname)


class ArrayNode(BaseArrayNode):

    #------------------------------------------------------------------------
    # BaseArrayNode interface
    #------------------------------------------------------------------------
    def seval(self):
        if client is not None:
            return client.rpc('eval', data=[self])[1][0]            
        else:
            raise Exception, 'cannot seval without a client'            
        
    def save(self, url):
        print 'STORING'
        if client is not None:
            client.rpc('store', urls=[url], data=[self])
            self.url = url
            msg, data = client.rpc('info', self.url)
            info = data[0]
            self.cached_shape = info['shape']
            self.cached_dtype = info.get('dtype', None)
        else:
            raise Exception, 'cannot save without a client'
        
    def save_temp(self):
        import uuid
        self.save("/tmp/" + str(uuid.uuid4()))
        
    def _graph_call(self, funcname, args, kw):
        # Return the a graphnode around the unbound method, and supplying
        # self as args[0].  In-place methodds (iadd,etc) are modified to
        # return self.
        node = ArrayNode(funcname, getattr(np.ndarray, funcname), (self,) + args, kw)
        self.add_listener(node)
        return node

    def _call(self, funcname, args, kw):
        # Since we need to return an actual concrete value, we have to eval
        # our upstream graph.
        if grapheval.DEBUG:
            print "[%d] %s: _call(%s, %r, %r)" % (id(self), self.dispname, funcname, args, kw)
        val = self.eval()
        return getattr(val,funcname)(*args, **kw)

    def _get_np_attr(self, attrname):
        # Convention: for attributes, instead of a funcname, use a
        # "." in front of the attribute name
        node = ArrayNode("." + attrname, getattr, (self, attrname))
        self.add_listener(node)
        return node

    def __setstate__(self, dict):
        super(ArrayNode, self).__setstate__(dict)
        try:
            self.func = getattr(np.ndarray, self.funcname)
        except:
            self.func = getattr(np, self.funcname)


    #------------------------------------------------------------------------
    # ArrayNode interface
    #------------------------------------------------------------------------

    def vec_eval(self):
        """ Evaluates/computes self._value based on upstream values.  However,
        instead of walking the graph and calling eval() on each GraphNode we
        encounter in an argument list, this builds a list of function calls
        which, when evaluated in sequence on each item in the upstream arrays,
        produces a value in the output.

        This is essentially a compiler that takes a graph of ArrayProxy/Node
        project and outputs code for a very braindead VM (basically a for
        loop).
        """
        self._compile()


    def _compile(self):
        pass

