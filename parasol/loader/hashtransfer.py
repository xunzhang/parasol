#! /usr/bin/python
#
# put loaded lines to hash slots
#

def hashfoo(i, j, npx, npy):
  '''
  Default hash strategy, mod npx and npy, sequence is transverse
  '''
  return (i % npx) * npy + j % npy

def putlines(lines, np, fmt = 'usr', hashfunc = hashfoo):
  '''
  Put lines to lineslotslst
    
  Notes
  -----
  Stuff in lines(list) is a (i, j, v), hash i,j to a slots index, than put (i, j, v) to lineslotslst
   
  Example
  -------
  >>> np = 8
  >>> lines = ['2\t4\t1\n', '4\t5\t2\n', '7\t6\t3\n', '9\t11\t4\n']
  >>> lst = putlines(lines, np)
  '''
  from parallel import npfact2D
  npx, npy = npfact2D(np)
  lineslotslst = [[] for i in xrange(np)]
  for line in lines:
    stf = []
    if fmt == 'usr':
      stf = [int(l) for l in line.strip('\n').split('\t')]
    if fmt == 'ussrt':
      l = line.strip('\n').split('\t')
      #for l in line.strip('\n').split('\t'):
      if l[2] == 'P':
        if l[3] == 'NULL' or l[3] == '':
          # 3.7 is avg rating
          stf = [int(l[0]), int(l[1]), 3.7]
        else:
          stf = [int(l[0]), int(l[1]), int(l[3])]
    if stf:
      tpl = (stf[0], stf[1], stf[2])
      lineslotslst[hashfunc(stf[0], stf[1], npx, npy)].append(tpl)
  return lineslotslst

if __name__ == '__main__':
  tpl = (28, 54)
  print hashfoo(tpl[0], tpl[1], 3, 4)
  lines = ['2\t4\t3\n', '23\t6\t4\n', '31\t3\t4\n', '14\t3\t4\n', '15\t3\t4\n', '61\t3\t4\n', '17\t3\t4\n', '81\t3\t4\n', '91\t3\t4\n']
  print putlines(lines, 4)
