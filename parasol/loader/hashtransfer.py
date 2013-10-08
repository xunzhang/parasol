#! /usr/bin/python
#
# put loaded lines to hash slots
#

def hashfoo(i, j, npx, npy):
  '''
  Default hash strategy, mod npx and npy, sequence is transverse
  '''
  return (hash(i) % npx) * npy + hash(j) % npy

def putlines(lines, np, parser = (lambda l : l), pattern = 'fmap', mix = False, hashfunc = hashfoo):
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
  from parasol.utils.parallel import npfactx, npfacty, npfact2D

  # default fmap pattern
  npx, npy = npfactx(np) 
  if pattern == 'fsmap': 
    npx, npy = npfact2D(np)
  if pattern == 'smap':
    npx, npy = npfacty(np)
  
  lineslotslst = [[] for i in xrange(np)]

  for line in lines:
    stf = []
    #if fmt == 'usr':
    #  stf = [int(l) for l in line.strip('\n').split('\t')]
    #if fmt == 'ussrt':
    #  l = line.strip('\n').split('\t')
    #  #for l in line.strip('\n').split('\t'):
    #  if l[2] == 'P':
    #    if l[3] == 'NULL' or l[3] == '':
    #      # 3.7 is avg rating
    #      stf = [int(l[0]), int(l[1]), 3.7]
    #    else:
    #      stf = [int(l[0]), int(l[1]), int(l[3])]
    stf = parser(line)
    if stf:
      if len(stf) == 2:
        tpl = (stf[0], stf[1], 1)
        lineslotslst[hashfunc(stf[0], stf[1], npx, npy)].append(tpl)
      elif mix:
        for item in stf[1:]:
          tpl = (stf[0], item, 1)
	  lineslotslst[hashfunc(stf[0], item, npx, npy)].append(tpl)
      else: 
        tpl = (stf[0], stf[1], stf[2])
        lineslotslst[hashfunc(stf[0], stf[1], npx, npy)].append(tpl)
  return lineslotslst

if __name__ == '__main__':

  from parasol.utils.lineparser import parser_a, parser_b, parser_ussrt

  tpl = (28, 54)
  print hashfoo(tpl[0], tpl[1], 3, 4)

  lines = ['2\t4\t3\n', '23\t6\t4\n', '31\t3\t4\n', '2\t5\t4\n', '14\t3\t4\n', '15\t3\t4\n', '61\t3\t4\n', '17\t3\t4\n', '81\t3\t4\n', '91\t3\t4\n']
  print putlines(lines, 4, parser_a('\t'), 'fsmap')

  lines = ['a\tb\n', 'a\tc\n', 'a\td\n', 'b\ta\n', 'b\td\n', 'c\tb\n', 'c\td\n', 'd\tc\n']
  print putlines(lines, 4, parser_b('\t'))
  print putlines(lines, 4, parser_b('\t'), 'fsmap')
  
  lines = ['a\tb\n', 'a\tc\td\n', 'b\ta\n', 'b\td\n', 'c\tb\td\n', 'd\tc\n']
  print putlines(lines, 4, parser_b('\t'), 'fsmap', True)

  lines = ['a\tb\tc\td\n', 'b\ta\td\n', 'c\tb\td\n', 'd\tc\n']
  print putlines(lines, 4, parser_b('\t'), 'fsmap', True)
