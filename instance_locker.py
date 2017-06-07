import os
import re
import etcd
import json
import time
import socket
import uuid
import argparse
import subprocess
import threading
from addict import Dict

import logging.config

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'filters': {},
    'formatters': {
        'standard': {
            'format': "%(asctime)s [%(levelname)s] %(message)s",
            'datefmt': "%Y-%m-%d %H:%M:%S"
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
    },
    'loggers': {
        'default': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': False,
        },
    },
}

logging.config.dictConfig(LOGGING)
logger = logging.getLogger('default')


def parse_endpoint(endpoint):
    e = re.match(r'^(?P<protocol>[^:]+)://(?P<host>[^:]+):(?P<port>\d+)', endpoint)
    if e is not None:
        d = e.groupdict()
        return {"protocol": d["protocol"], "host": d["host"], "port": int(d["port"])}
    else:
        return None


class SubCommand(object):

    def __init__(self, command, *args, **kwargs):
        self.env = os.environ.copy()
        self.env.update({'LD_LIBRARY_PATH': ''})
        self.process = subprocess.Popen(command, env=self.env, shell=True)

    def is_running(self):
        return bool(self.process.poll() is None)

    def exit_with_err(self):
        return bool(self.process.poll() != 0)

    def stop(self):
        while self.is_running():
            logger.info("send SIGTERM to child_process")
            self.process.terminate()
            time.sleep(0.5)

        logger.info("child_process is stoped")


class InstanceLocker(object):

    def __init__(self, endpoint=None, renewSecondsPrior=5, timeout=None):
        endpoint = endpoint if endpoint is not None else os.environ.get('ETCD_ENDPOINT', 'http://127.0.0.1:4001')
        conf = parse_endpoint(endpoint)
        self.etcd_cfg = conf
        self.etcdClient = None
        self.base_path = '/goodrain/locks/instances'
        self.client = self.get_etcd_cli()

    def get_etcd_cli(self):
        if self.etcdClient is None:
            self.etcdClient = etcd.Client(host=self.etcd_cfg.get('host'), port=self.etcd_cfg.get('port'), allow_redirect=True)
        return self.etcdClient

    def get_lock(self, instance_name):
        key = self.base_path + '/' + instance_name.lstrip('/')
        try:
            return self.client.get(key)
        except etcd.EtcdKeyNotFound:
            return None

    def add_lock(self, instance_name, value, ttl=60):
        key = self.base_path + '/' + instance_name.lstrip('/')
        try:
            self.client.write(key, value, prevExist=False, recursive=True, ttl=ttl)
        except etcd.EtcdAlreadyExist:
            return False
        return True

    def update_lock(self, instance_name, value, ttl=60):
        key = self.base_path + '/' + instance_name.lstrip('/')
        self.client.write(key, value, ttl=ttl)
        return True

    def drop_lock(self, instance_name):
        key = self.base_path + '/' + instance_name.lstrip('/')
        try:
            self.client.delete(key, prevExist=True)
        except Exception as e:
            print(e)


class LeaderNotFound(Exception):
    pass


class LeaderElectionRecord(object):

    def __init__(self, etcd_obj, *args, **kwargs):
        super(LeaderElectionRecord, self).__init__(*args, **kwargs)
        self.holder_identity = etcd_obj.holder_identity
        self.acquire_time = etcd_obj.acquire_time
        self.renew_time = etcd_obj.renew_time
        self.lease_duration_seconds = etcd_obj.lease_duration_seconds
        self.renew_deadline = etcd_obj.renew_deadline
        self.token = etcd_obj.token

    def need_update(self):
        locked_time = time.time() - self.renew_time
        return bool(locked_time > self.renew_deadline)

    def as_dict(self):
        data = {
            "holder_identity": self.holder_identity, "token": self.token,
            "acquire_time": self.acquire_time, "renew_time": self.renew_time,
            "lease_duration_seconds": self.lease_duration_seconds, "renew_deadline": self.renew_deadline
        }
        return data


class LeaderElector(object):
    DEFAULT_RETRY_PERIOD = 2
    DEFAULT_LEASE_DURATION = 15
    DEFAULT_RENEW_DEADLINE = 10

    def __init__(self, name, identity=None, *args, **kwargs):
        super(LeaderElector, self).__init__(*args, **kwargs)
        self.INSTANCE_NAME = name
        self.RETRY_PERIOD = self.DEFAULT_RETRY_PERIOD
        self.RENEW_DEADLINE = self.DEFAULT_RENEW_DEADLINE
        self.LEASE_DURATION = self.DEFAULT_LEASE_DURATION

        self.identity = socket.gethostname() if identity is None else identity
        self.token = str(uuid.uuid4())
        self.locker = InstanceLocker()

    def get_leader(self):
        leader_obj = self.locker.get_lock(self.INSTANCE_NAME)
        if leader_obj is None:
            raise LeaderNotFound("leader of {} not found".format(self.INSTANCE_NAME))
        etcd_obj = Dict(json.loads(leader_obj.value))
        leader = LeaderElectionRecord(etcd_obj)
        return leader

    def create_leader(self):
        create_time = time.time()
        data = {
            "holder_identity": self.identity, "token": self.token,
            "acquire_time": create_time, "renew_time": create_time,
            "renew_deadline": self.RENEW_DEADLINE,
            "lease_duration_seconds": self.LEASE_DURATION
        }
        return self.locker.add_lock(self.INSTANCE_NAME, json.dumps(data), ttl=self.LEASE_DURATION)

    def update_leader(self, renew_time=None):
        data = self.leader.as_dict()
        data.update({"renew_time": renew_time})
        self.locker.update_lock(self.INSTANCE_NAME, json.dumps(data), ttl=self.LEASE_DURATION)

    @property
    def is_leader(self):
        return bool(self.leader.holder_identity == self.identity and self.leader.token == self.token)

    def acquire(self):
        def make_leader():
            if self.create_leader():
                logger.info("sucessfully acquired lease {}/{}".format(self.INSTANCE_NAME, self.identity))
                return True
            else:
                logger.error("error acquire lease {}/{}".format(self.INSTANCE_NAME, self.identity))
                return False

        while True:
            try:
                self.leader = self.get_leader()
                if not self.is_leader:
                    logger.info("lock is held by {} and has not yet expired".format(self.leader.holder_identity))
            except LeaderNotFound:
                success = make_leader()
                if success:
                    break

            time.sleep(self.RETRY_PERIOD)

    def renew(self, command, stop):
        p = SubCommand(command)

        while True:
            if stop.is_stoped is True:
                logger.info("stopped")
                p.stop()
                break
            if not p.is_running():
                logger.info("child_process is not running, break")
                break
            try:
                self.leader = self.get_leader()
                logger.debug("check lock and renew")
                if self.is_leader:
                    renew_time = time.time()
                    if self.leader.need_update():
                        self.update_leader(renew_time=renew_time)
                        logger.info("successful renew lease {}/{}".format(self.INSTANCE_NAME, self.identity))
                    time.sleep(self.RETRY_PERIOD)
                else:
                    p.stop()
                    break
            except LeaderNotFound as e:
                logger.error("failed renew leader, reason: {}".format(e))
                p.stop()
                break

    def release(self):
        self.locker.drop_lock(self.INSTANCE_NAME)

    def block_command(self, command):
        class StopLoop(object):
            is_stoped = False

        stop = StopLoop()

        renew_thread = threading.Thread(target=self.renew, args=(command, stop))
        renew_thread.start()
        while True:
            try:
                if not renew_thread.isAlive():
                    break
                renew_thread.join(1)
            except KeyboardInterrupt:
                stop.is_stoped = True
        logger.info("renew_thread finished")

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args, **kwargs):
        logger.info("stop leader lease")
        return self.release()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--command', action='store', help='the command to run')
    parser.add_argument('-n', '--name', action='store', help="specify an instance mutex lock name", )
    args = parser.parse_args()
    with LeaderElector(args.name) as locker:
        locker.block_command(args.command)
