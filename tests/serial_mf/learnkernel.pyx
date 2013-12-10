import numpy as np
cimport numpy as np

cpdef inline dot(np.ndarray[np.float64_t, ndim = 1] p, np.ndarray[np.float64_t, ndim = 1] q):
    cdef double result = 0
    cdef int i = 0
    cdef double t1 = 0., t2 = 0.
    cdef int length = p.size
    for i in xrange(length):
        t1 = p[i]
        t2 = q[i]
        result += t1 * t2
    return result

cpdef np.float64_t estimate(np.float64_t miu, 
	np.float64_t usr_bias,
	np.float64_t item_bias,
	np.ndarray[np.float64_t, ndim = 1] p, 
	np.ndarray[np.float64_t, ndim = 1] q):
    cdef np.float64_t r = miu + usr_bias + item_bias + dot(p, q)
    #cdef np.float64_t r = miu + usr_bias + item_bias + np.dot(p, q)
    #r = min(5., r)
    #r = max(1., r)
    return r

cpdef learn(np.float64_t rating,
        np.float64_t alpha,
	np.float64_t beta,
        np.float64_t miu,
	np.float64_t usr_bias,
	np.float64_t item_bias,
	np.ndarray[np.float64_t, ndim = 1] p, 
	np.ndarray[np.float64_t, ndim = 1] q):
    cdef np.float64_t tmp, e, delta_ubias, delta_ibias
    cdef int sz = p.shape[0]
    cdef np.ndarray[np.float64_t, ndim = 1] delta_p = np.empty(sz, dtype = np.float64)
    cdef np.ndarray[np.float64_t, ndim = 1] delta_q = np.empty(sz, dtype = np.float64)
    tmp = estimate(miu, usr_bias, item_bias, p, q) 
    e = rating - tmp
    for k in range(sz):
        delta_p[k] = alpha * (2 * e * q[k] - beta * p[k]) 
        delta_q[k] = alpha * (2 * e * p[k] - beta * q[k])
    delta_ubias = alpha * (2 * e - beta * usr_bias)
    delta_ibias = alpha * (2 * e - beta * item_bias)
    return (delta_p, delta_q, delta_ubias, delta_ibias)
