import subprocess
import os

def start_redis(port, data_dir, data_file='redis.db', logfile='redis.log'):
    base_config = os.path.join(os.path.dirname(__file__), 'redis.conf')
    logfile = open(os.path.join(data_dir, logfile), 'w+')
    with open(base_config) as f:
        redisconf = f.read()
    redisconf = redisconf % {'port' : port,
                             'dbdir' : data_dir,
                             'dbfile' : data_file}
    proc = subprocess.Popen(['redis-server', '-'],
                            stdout=logfile,
                            stdin=subprocess.PIPE,
                            stderr=logfile)
    proc.stdin.write(redisconf)
    proc.stdin.close()
    return proc
                     

class RedisProcess(object):
    def __init__(self, port, data_dir, data_file='redis.db', logfile='redis.log'):
        self.proc = start_redis(port, data_dir,
                                data_file=data_file, logfile=logfile)
    def __del__(self):
        self.proc.kill()
        self.proc.communicate()
        

                
