
name-server              
-----------

   maps name to url with array-server and id of array on that machine
   uniqueness is guaranteed on a single name-server only (for now).  The url +
   id is already a unique name, this is just a short-hand for use in
   applications.  It's like a variable name in an application as opposed to a
   memory address.  


NDTable
-------

An NDTable object holds and manages the relationship between a high-level,
array-oriented view of a collection of data buffers and the low-level access
and interpretation of those data which can be used for compute. 

NDTables are composed of a number of other ndtables.  In other-words, there is
a mapping between the shape and attribute lay-out of this table and children
ndtables.    This recursive definition ends at leaf ndtables which are
fundamentally either: 

  * generated via an expression graph
  * dtyped and shaped byte-wrapper (the shaping happens via a bijective 
    function that maps (i,j,k...) to the Nth byte with striding as a special case). Kinds of byte-buffers:

     * General abstract "storage" (copies to a buffer through the interface)
        * stream (read N bytes)
        * seekable (read byte N to byte M)
     * RAM
        * main memory
        * gpu memory
     * Random IO (seekable, block-seeking or byte-seeking)
        * spinning disk
        * solid state disk
     * Sequential IO (buffered)
        * socket 
        * usb
        * RS-232
        * line-in
        * frame-grabber
        * other byte-based measurement data-source
     * remote memory
        * url + 
           * disk / file / offset
           * process / pointer
           * shared memory segment
        * this is used to map data actually needed on a machine with no
          ndtable wrapper (normally one would wrap the ndtable on the
          machine where the data lives and use the remote ndtable
          facility).
     * byte-buffers have affinity information providing latency and
       bandwidth that a compute scheduler can use
     * byte-buffers are not growable (and can be readonly or writeable)
     * to grow an ndtable you create a new byte-buffer and map it to the
       ndtable   
     * byte-buffers can have an optional, separate selection mask with
       it's own dtype and "stride" function but has the same shape as the
       underlying mask.

  * Children can be ndtables (either named with respect to a name-server or
    identified uniquely via url + id or if only id than url is local) ---
    computations will be pushed where possible to the node where this array
    lives

DType 
-----

A dtype is low-level data-type or dynamic type.  It is a byte-level dynamic
typing system that defines how a collection of bytes should be interpreted by
the calculation engine.  In contrast to static typing systems which are applied
at compilation time, the dtype provides information and compute dispatch
capability layered over the bits at run-time.

Each byte-buffer has a corresponding dtype which describes how the individual
"bytes" should be interpreted during compute / processing.  To the standard
NumPy dtypes we add: 

  * bit-pattern dtypes (a bit-pattern reserved for a mask)
  * masked dtypes: a bit-mask added for every N elements where N is a multiple
    of some word-size like 32 or 64)
  * level dtypes
  * reference dtypes (what is stored in the memory of a dtype is a reference to
    the actual data)

     * arbitrary precision floats
     * arbitrary precision complex numbers
     * big integers
     * vararray dtypes (these are "ragged" dtypes where the memory is actually
       stored elsewhere and managed separately)
     * varutf8 dtypes
     * varbytes dtypes
     * reference to another dtype 
     * reference to another dtype-mapped buffer (this allows C-style arrays to
       be understood) 
     * far-reference dtypes (url + (disk, file, offset) or
                     url + (process, pointer))
     * general Object

  * array dtypes which take an index-mapping function to map the 'i,j,k,...'
    element to the linear N'th element of the array

     * special-case for strided cases
     * special-case for C-contiguous
     * special-case for F-contiguous
     * array dtypes maintain their character when constructed so that a (2,)
       array of (2,3) array-dtypes continues to have a shape of (2,) where
       every element is a (2,3) array.
     * array dtypes can also have a separate, associated selection buffer which
       uses the same index-mapping function but a separate dtype and location
       for the memory for a selection "mask"

  * expression dtypes (including expressions with null storage, i.e. generated
    data).  
  * derived fields in structured arrays...
  * cell dtypes (result based on domain of another ndtable)
  * block-expression dtypes (these dtypes are similar to expression dtypes
    except they take N elements of the underlying storage and return M elements
    of the result).  Several cases: 

     * N and M fixed --- easiest case (no index required)
     * N fixed M varying 
     * N varying M fixed
     * N and M both varying 
     * streaming
     * Index can be added to a block-expression dtype to navigate and provide ability to reason about shape prior to run-time.  Otherwise there are "unknown shape values" (-1)
     * CSV-parsing is an example of N varying but M fixed.
     * Notice that a "transformation function" is subsubmed by the block-expression dtypes. 

  * Notice that a traditional dynamically allocated C-array with
    separate memory buffers can be understood with a memory
    buffer dtype that is a pointer to pointer to fundamental
    with a pointer to pointer to dtype  

  * Notice also that a traditional NumPy array is contained
    entirely in the array-dtype 

Domains 
-------

 * Basically, things that will be used to index an ndtable or define its chunking. 
 * Arithmetic (basically tuples of slice objects); these can be interleaved with step
 * Sparse (fancy-indexing really -- actual indexes that will be selected)
 * Cartesian index (Tensor product of specific per-dimension indexes)

Dimensions
----------

 * An NDTable contains a mapping between domains and chunks or dimensions and
   chunks. 
 * An NDTable can contain only one chunk (i.e. generated arrays, ...
 * An NDTable also contains a dimension mapping that creates the shape from the
   underlying ndtable chunks.  

Have properties  of ordering and indexing.  Standard dimensions are "integer"
based and implicit.  Sparse dimensions replace one or more attributes in
another ndtable and one or more dimensions.   Dimensions can be named (sparse
dimensions get a default name consisting of their attributes).  Sparse
dimensions can be ordered or unordered (i.e. categorical).

Dimensions can be *labeled* by something that maps a hashable type to an
integer: 

 * by a dictionary
 * 1-d, 2-attribute ndtable
 * a 2-d ndtable
 * a 1-d 2-attribute ndarray
 * a 2-d ndarray  or an ndarray

An NDTable is indexed via slicing on "dimensions" and accessing attributes.

**Question**:   Should accessing an attribute be the same as
accessing dimensions?  

Yes, we should treat the attribute list as the last dimension.   One reason: the idea of sparse dimensions could be applied to a any non-attribute based.   Attributes become just another labeled dimension. 
        
Chunking patterns
-----------------

  * Chunking by attributes -- a new chunk for a set of attributes
  * Chunking by domains -- a chunk is defined by a particular
    partition of the ND and attribute space

     * Special cases are chunking along a specific dimension or sub-set of
       dimensions (i.e. entire range in other dimensions)
  
  * Chunking based on a mapping of a subset of the dimensions to a dimensional
    space that matches the underlying ndtable.  The idea here is to support
    something like Z-order chunking for at least a sub-set of the dimensions.
    In the simplest case the underlying buffer would be 1-d (but it could have
    additional "dimensionality" with just the leading dimension being un-rolled
    into the other dimensions). 
 
All of these chunking patterns are subsumed under the common
chunking rule (assuming attributes are seen as a dimension): 

    Chunking divides up the ndtable along 1 or more dimensions of a mapped
    index set.   The mapping is Z^n to Z^m where n and m can be different.
    the mapping can be the identity.

    e.g. : 
         (i,j,k,...) -> (I,J,K,...)   

    then the partition is on blocks of (I,J,K,...)

Partitions can be:

  * concatenative (almost) uniform (define the chunk_size (100,200,100) +
    overlap factor (50,20,20) or just 50 )
  * concatenative map-based:  start,end N-d coordinates in a hash-table
    pointing to N-d ndtables
  * axis-based:  chunk_size + overlap and an axis or set of axes
  * axis-map-based:  start, end n-d coordinates and a set of n
    axes in a hash-table pointing to (N-n)-d ndtables.

Random thoughts:

  * Chapel's "vectorize" primitives are interesting consisting of standard
    "zip" promotion and tensor product promotion.

     Should consider returning a vectorize function with tensor promotion
     that takes the input arguments and returns appends newaxis arguments.

     Suppose you have N-input arguments all 1-dim  then the kth input
     argument has shape (1,)*(N-k-1) + arg.shape + (1,)*k  with k starting at
     0 and going to N-1

        N = 2:   arg0 = (5,) and arg1 = (7,)

          (1,5) and (7,1)

        N = 3:  arg0 = (5,) and arg1 = (6,) and arg2 = (7,)

          (1,1,5) and (1,6,1) and (7,1,1)


Read:   http://chapel.cray.com/spec/spec-0.775.pdf

Chapel also has the notion of locales which are compute-and-memory nodes.  We
will borrow this idea. 


