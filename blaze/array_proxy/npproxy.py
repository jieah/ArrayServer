""" Wrapper around the numpy module which returns ArrayNodes if any inputs are
ArrayNodes.
"""



_funcs = []
_ctors = []
#------------------------------------------------------------------------
# Array creation & manipulation functions
#------------------------------------------------------------------------

# Array creation funcs always return ArrayProxies
_ctors.extend(("ones,zeros,arange,linspace").split(","))

_funcs.extend(("ones_like,zeros_like,"
    "array,asarray,asfarray,asfortranarray,asanyarray,ascontiguousarray,asmatrix,"
    "copy,frombuffer,fromfunction,fromiter,fromstring,"
    "reshape,ravel,rollaxis,swapaxes,transpose,"
    "atleast_1d,atleast_2d,atleast_3d,broadcast,broadcast_arrays,"
    "expand_dims,squeeze,require,"
    "column_stack,concatenate,dstack,hstack,vstack,"
    "array_split,dsplit,hsplit,split,vsplit,"
    "tile,repeat,delete,insert,append,resize,trim_zeros,unique,"
    "fliplr,flipud,reshape,roll,rot90").split(","))

#------------------------------------------------------------------------
# Indexing functions
#------------------------------------------------------------------------

_funcs.extend(("c_,r_,s_,nonzero,where,indices,ix_,ogrid,"
    "unravel_index,diag_indices,diag_indices_from,mask_indices,"
    "tril_indices,tril_indices_from,triu_indices,triu_indices_from,"
    "take,choose,compress,diag,diagonal,select,"
    "place,put,putmask,fill_diagonal,ndenumerate,ndindex,flatiter").split(","))

#------------------------------------------------------------------------
# Math functions
#------------------------------------------------------------------------

_funcs.extend(("sin,cos,tan,arcsin,arccos,arctan,hypot,arctan2,"
    "degrees,radians,unwrap,deg2rad,rad2deg,sinh,cosh,tanh,"
    "arcsinh,arccosh,arctanh,"
    "around,round,rint,fix,floor,ceil,trunc,"
    "prod,sum,nansum,cumprod,cumsum,diff,ediff1d,gradient,cross,trapz,"
    "exp,expm1,exp2,log,log10,log2,log1p,logaddexp,logaddexp2,"
    "i0,sinc,signbit,copysign,frexp,ldexp,"
    "add,reciprocal,negative,multiply,divide,power,subtract,"
    "true_divide,floor_divide,fmod,mod,modf,remainder,"
    "angle,real,imag,conj,"
    "convolve,clip,sqrt,square,absolute,fabs,sign,maximum,"
    "minimum,nan_to_num,real_if_close,interp").split(","))



def _wrap(func, name=""):
    from array_proxy import ArrayNode, BaseArrayNode
    def wrapped_func(*args, **kw):
        if len(args) > 0 and isinstance(args[0], tuple):
            arrayargs = args[0]
        else:
            arrayargs = args
        parent_nodes = [arg for arg in arrayargs if isinstance(arg, BaseArrayNode)]
        parent_nodes.extend(val for val in kw.values() if isinstance(val, BaseArrayNode))
        if len(parent_nodes) > 0:
            node = ArrayNode(name, func, args, kw)
            for n in parent_nodes:
                n.add_listener(node)
            return node
        else:
            return func(*args, **kw)
    return wrapped_func

import numpy as np

for fn in _funcs:
    exec "%s = _wrap(np.%s, name='%s')" % (fn,fn,fn)

def _wrap_ctor(func, name=""):
    from array_proxy import ArrayNode, BaseArrayNode
    def wrapped_ctor(*args, **kw):
        if len(args) > 0 and isinstance(args[0], tuple):
            arrayargs = args[0]
        else:
            arrayargs = args
        parent_nodes = [arg for arg in arrayargs if isinstance(arg, BaseArrayNode)]
        parent_nodes.extend(val for val in kw.values() if isinstance(val, BaseArrayNode))
        node = ArrayNode(name, func, args, kw)
        if len(parent_nodes) > 0:
            for n in parent_nodes:
                n.add_listener(node)
        return node
    return wrapped_ctor

for fn in _ctors:
    exec "%s = _wrap_ctor(np.%s, name='%s')" % (fn,fn,fn)


