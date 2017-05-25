import numpy

def cast(arr,valid):
    res = numpy.zeros((arr.shape[0],valid.size),dtype=arr.dtype)
    res[:,valid] = arr
    return res

def normalize(arr):
    m = arr.mean(axis=0)
    s = arr.std(axis=0)
    valid = numpy.fabs(s)>1e-6
    fun = lambda x: (x[:,valid]-m[valid])/s[valid]
    ifun = lambda y: cast(y * s[valid] + m[valid],valid)
    return fun(arr),fun,ifun

def normalize_output(arr):
    z = numpy.log(1+arr[:,1])
    m = z.mean(axis=0)
    s = z.std(axis=0)
    fun = lambda z: (numpy.log(1+arr[:,1])-m)/s
    ifun = lambda z: numpy.expm1(z*s+m)
    return fun(arr),fun,ifun


def shuffle_list(arr_list):
    m = arr_list[0].shape[0]
    IQ = numpy.random.permutation(m)
    return tuple(arr[IQ] for arr in arr_list)