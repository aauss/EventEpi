import collections

def group_by(group_fun, collection):
    out = {}
    if isinstance(group_fun, basestring):
        prop = group_fun
        group_fun = lambda k: k[prop]
    for item in collection:
        group_key = group_fun(item)
        out[group_key] = out.get(group_key, []) + [item]
    return out
    
def flatten(li, depth=-1):
    for subli in li:
        if isinstance(subli, (list, set, tuple)) and depth != 0:
            for it in flatten(subli, depth - 1):
                yield it
        else:
            yield subli

# Generic helpers:
def squash_dict(d, delimiter='/', crunch=False, layers=-1):
    """
    Combine recursively nested dicts into the top level dict by prefixing their
    keys with the top level key and delimiter.
    Use the layers parameter to limit the recursion depth.
    Adding the prefixed keys could collide with keys already in the
    top level dictionary, use crunch to suppress errors and replace the
    top level keys when this happens.
    """
    dout = {}
    for k, v in d.items():
        if isinstance(v, dict) and layers != 0:
            for vk, vv in squash_dict(v, delimiter, crunch, layers - 1).items():
                new_key = k + delimiter + vk
                if not crunch and new_key in d.keys():
                    raise Exception("Collision when squashing dict.")
                dout[new_key] = vv
        else:
            dout[k] = v
    return dout

class memoized(object):
    """
    Decorator. Caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned
    (not reevaluated).
    """
    def __init__(self, func):
       self.func = func
       self.cache = {}
    def __call__(self, *args):
       if not isinstance(args, collections.Hashable):
          # uncacheable. a list, for instance.
          # better to not cache than blow up.
          return self.func(*args)
       if args in self.cache:
          return self.cache[args]
       else:
          value = self.func(*args)
          self.cache[args] = value
          return value
    def __repr__(self):
       '''Return the function's docstring.'''
       return self.func.__doc__
    def __get__(self, obj, objtype):
       '''Support instance methods.'''
       return functools.partial(self.__call__, obj)

def resource_url(id, set_name="devtest", branch="fetch_4-18-2014"):
    if isinstance(id, dict):
        id = id['_id']
    return "https://github.com/ecohealthalliance/corpora/blob/" +\
        branch + "/healthmap/" + set_name + "/" + id + ".md"
