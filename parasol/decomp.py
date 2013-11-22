def npfactx(np):
  return np, 1
  
def npfacty(np):
  return 1, np

def npfact2d(np, row_master = True):
  import math
  upbnd = int(math.sqrt(np))
  while upbnd - 1:
    if np % upbnd == 0:
      if not row_master:
        return upbnd, np / upbnd
      else:
        return np / upbnd, upbnd
    else:
      upbnd -= 1
  if row_master:
    return np, 1
  else:
    return 1, np
