
def file_load_lines_impl(fn, st, en):
    f = open(fn, 'rb')
    offset = st
    if offset:
        f.seek(offset - 1)
        l = f.readline()
        # add edge offset. if no edge, add 1:'\n'
        offset += len(l) - 1
    while offset < en:
        l = f.readline()
        offset += len(l)
        yield l
    f.close()

def file_partition(fn, np):
    import os
    import functools
    func_loaders = []
    sz = os.stat(fn).st_size
    nbk = np
    bk_sz = sz / nbk
    for i in xrange(nbk):
        s = i * bk_sz
        if i == nbk - 1:
            e = sz
        else:
            e = (i + 1) * bk_sz
        func_loaders.append(functools.partial(file_load_lines_impl, fn, s, e))
    return func_loaders

def files_load_lines_impl(name_lst, displs, st, en):
    import sys
    if en < st:
        print 'error in fs_ld_lines with en < st'
        sys.exit(1)
  
    # to locate files index to load from
    fst = 0
    fen = 0
    for i in xrange(len(name_lst)):
        if st >= displs[i]:
            fst = i
        if en > displs[i + 1]:
            fen = i + 1 
    flag = False
    # load from files
    for fi in xrange(fst, fen + 1):
        if flag:
            offset = 0
        else:
            offset = st - displs[fi]
        f = open(name_lst[fi], 'rb')
        if offset:
            f.seek(offset - 1)
            l = f.readline()
            offset += len(l) - 1
        if fi == fen:
            while offset + displs[fi] < en:
                l = f.readline()
                offset += len(l)
                yield l
        else:
            flag = True
            while True:
                l = f.readline()
                if not l: break
                yield l 
        f.close()
  
def files_partition(name_lst, np, pattern = '', blk_sz = 8):
    import os
    import functools
    if pattern == ('linesplit' or 'fvec'):
        blk_sz = 1
    np = np * blk_sz 
    func_loaders = []
    displs = [0] * (len(name_lst) + 1)
    for i in xrange(len(displs) - 1):
        displs[i + 1] = displs[i] + os.stat(name_lst[i]).st_size
    sz = displs[-1]
    nbk = np
    bk_sz = sz / np
    for i in xrange(nbk):
        s = i * bk_sz
        if i == nbk - 1:
            e = sz
        else:
            e = (i + 1) * bk_sz
        func_loaders.append(functools.partial(files_load_lines_impl, name_lst, displs, s, e))
    return func_loaders
