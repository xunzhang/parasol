# generate availiable port

def is_availiable(port):
    import os
    cmd = 'netstat -tuln | grep LISTEN | cut -f 2 -d :'
    tmp = os.popen(cmd)
    content = tmp.read()
    content = content.strip('\n').split('0.0.0.0')
    plst = [item.strip('\n').strip(' ') for item in content]
    while '' in plst:
        plst.remove('')
    plst = [int(item) for item in plst]
    tmp.close()
    if port in plst:
        return False
    elif port == 7777:
        return False
    else:
        return True

def getport():
    import os
    import random
    port = random.randint(5000, 65535)
    while not is_availiable(port):
        port = random.randint(5000, 65535)
    return port

def getports():
    portlst = []
    for i in xrange(4):
        portlst.append(getport())
    return portlst
