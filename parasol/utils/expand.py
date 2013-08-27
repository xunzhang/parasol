#! /usr/bin/python
#
# expand file names to a python list
#

import os

def _expd_dir_rec(dn):
  import glob
  flst = []
  if not dn.endswith('/'):
    dn += '/'
  items = glob.glob(dn + '*')
  for it in items:
    if os.path.isfile(it):
      flst.append(it)
    else:
      flst += _expd_dir_rec(it)
  return flst
  
def expd_f_lst(fns):
  '''
  Expand fns to a file name list
  
  Parameters
  ----------
  fns : can be 'demo.txt', 'demo_dir', ['demo.txt', 'demo_dir']

  Returns
  -------
  a list of file names
  '''
  import glob
  import types
  flst = [] 
  # expect a list contains files and dirs
  if isinstance(fns, list):
    for it in fns:
      if os.path.isfile(it):
        flst.append(it)
      else:
        flst += _expd_dir_rec(it)
    return flst
  # a file
  elif os.path.isfile(fns):
    return [fns]
  # a dir
  elif os.path.isdir(fns):
    return _expd_dir_rec(fns)
  # such as '/home/wuhong/*.csv'
  else:
    return glob.glob(fns)
