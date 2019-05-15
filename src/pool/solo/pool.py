#!/usr/bin/env python

# Copyright (C) 2019 MentalCollatz
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import socket
import threading
import time
import signal

import config
import rpc
from template import BlockTemplate

# colors
VIOLET = '\033[95m'
BLUE = '\033[94m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'

ENDC = '\033[0m'
BOLD = '\033[1m'
UNDERLINE = '\033[4m'

threadcount = 0

class GetTemplate(threading.Thread):
    def __init__(self, callback):
        threading.Thread.__init__(self)

        self.shutdown_flag = threading.Event()
        self.longpollid = None
        self.last_errno = None
    
    def run(self):
        global threadcount
        threadcount += 1
        print('%s%s Thread #%s started (GetTemplate)%s' % (time.strftime("[%Y-%M-%d %H:%M:%S]"), VIOLET, self.ident, ENDC))
        while not self.shutdown_flag.is_set():
            try:
                template = rpc.get_block_template(self.longpollid)
                if "coinbaseaux" not in template:
                    template["coinbaseaux"] = {}
                template["coinbaseaux"]["cbstring"] = config.get("cbstring")
                callback(template)
                self.longpollid = template["longpollid"]
                if self.last_errno != 0:
                    print("%s >>> successfully acquired template <<< %s" % (time.strftime("[%Y-%M-%d %H:%M:%S]") + YELLOW + BOLD, ENDC))
                    self.last_errno = 0
            except (rpc.RpcError, socket.error) as e:
                if self.last_errno == 0:
                    callback(None)
                if e.errno != self.last_errno:
                    self.last_errno = e.errno
                    print("%s%s %s (errno %d)" % (RED, time.strftime("[%Y-%M-%d %H:%M:%S]"), e.strerror, e.errno))
                time.sleep(1)
        threadcount -= 1
        print('%s%s Thread #%s stopped (GetTemplate)%s' % (time.strftime("[%Y-%M-%d %H:%M:%S]"), VIOLET, self.ident, ENDC))

class Manager(threading.Thread):
    def __init__(self, cbscript):
        threading.Thread.__init__(self)
        self.shutdown_flag = threading.Event()
        self.cbscript = cbscript
        self.template = None
        self.extra_nonce = 0
        self.miners = []
        self.cond = threading.Condition()
    
    def add_miner(self, miner):
        with self.cond:
            self.miners.append(miner)
            self.cond.notify()
    
    def remove_miner(self, miner):
        with self.cond:
            self.miners.remove(miner)
    
    def push_template(self, template):
        with self.cond:
            if template is None:
                self.template = None
            else:
                self.template = BlockTemplate(template, self.cbscript)
            self.extra_nonce = 0
            for miner in self.miners:
                miner.next_refresh = 0
            self.cond.notify()
    
    def run(self):
        global threadcount
        threadcount += 1
        print('%s%s Thread #%s started (Manager)%s' % (time.strftime("[%Y-%M-%d %H:%M:%S]"), VIOLET, self.ident, ENDC))
        while not self.shutdown_flag.is_set():
            with self.cond:
                now = time.time()
                next_refresh = now + 1000
                for miner in self.miners:
                    if miner.next_refresh < now:
                        miner.push_work(self.template, self.extra_nonce)
                        self.extra_nonce += 1
                    next_refresh = min(next_refresh, miner.next_refresh)
                wait_time = max(0, next_refresh - time.time())
                self.cond.wait(wait_time)
        threadcount -= 1
        print('%s%s Thread #%s stopped (Manager)%s' % (time.strftime("[%Y-%M-%d %H:%M:%S]"), VIOLET, self.ident, ENDC))            

class Miner(threading.Thread):
    def __init__(self, conn, manager):
        threading.Thread.__init__(self)
        self.shutdown_flag = threading.Event()
        self.conn = conn
        self.manager = manager
        self.lock = threading.Lock()
        self.conn_lock = threading.Lock()
        self.work_items = []
        self.next_refresh = 0
        self.refresh_interval = 10
        self.lastodo = 0
        self.lasttarget = "0"
        manager.add_miner(self)
        self.start()

    def push_work(self, template, extra_nonce):
        if template is None:
            workstr = "work %s %s %d" % ("0"*64, "0"*64, 0)
        else:
            work = template.get_work(extra_nonce)
            workstr = "work %s %s %d" % (work, template.target, template.odo_key)
        with self.lock:
            if template is None:
                self.work_items = []
            else:
                self.work_items.insert(0, (work, template, extra_nonce))
                if len(self.work_items) > 2:
                    self.work_items.pop()
            self.next_refresh = time.time() + self.refresh_interval       
        if (self.lastodo != template.odo_key):
            self.lastodo = template.odo_key
            print("%s%s ODOkey %s>>> %s <<<" % (time.strftime("[%Y-%M-%d %H:%M:%S]"),GREEN + BOLD, ENDC, self.lastodo))
        if (self.lasttarget != template.target and config.get("swtarget")):
            self.lasttarget = template.target
            print("%s%s Target %s%s" % (time.strftime("[%Y-%M-%d %H:%M:%S]"), BLUE, ENDC, self.lasttarget))           
        try:
            self.send(workstr)
        except socket.error as e:
            # let the other thread clean it up
            pass

    def send(self, s):
        with self.conn_lock:
            self.conn.sendall((s + "\n").encode())

    def submit(self, work):
        with self.lock:
            for work_item in self.work_items:
                if work_item[0][0:152] == work[0:152]:
                    template = work_item[1]
                    extra_nonce = work_item[2]
                    submit_data = work + template.get_data(extra_nonce)
                    break
            else:
                return "stale"
        try:
            return rpc.submit_work(submit_data)
        except (rpc.RpcError, socket.error) as e:
            print("failed to submit: %s (errno %d)" % (e.strerror, e.errno));
            return "error"

    def run(self):
        global threadcount
        threadcount += 1
        print('%s%s Thread #%s started (Miner)%s' % (time.strftime("[%Y-%M-%d %H:%M:%S]"), VIOLET, self.ident, ENDC))
        while not self.shutdown_flag.is_set():
            try:
                data = self.conn.makefile().readline().rstrip()
                if not data:
                    break
                parts = data.split()
                command, args = parts[0], parts[1:]
                if command == "submit" and len(args) == 1:
                    result = self.submit(*args)
                    self.send("result %s" % result)
                else:
                    print("unknown command: %s" % data)
            except socket.error as e:
                break

        # Clean shutdown
        threadcount -= 1
        print('%s%s Thread #%s stopped (Miner)%s' % (time.strftime("[%Y-%M-%d %H:%M:%S]"), VIOLET, self.ident, ENDC))
        self.manager.remove_miner(self)
        self.conn.close()

def service_start():
    print('')
    print(GREEN + '*******************************************************************')
    print('***    Starting Pool, please press Ctrl+C to shutdown           ***')
    print('*******************************************************************' + ENDC)
  
def service_shutdown(signum, frame):
    global threadcount
    print(RED + ' KeyboardInterrupt' + ENDC)
    print(GREEN + '*******************************************************************')
    print('***    Shutting down Pool, please stop your Miner to finish     ***')
    if threadcount < 3:
        print('***    ' + YELLOW +'No Miner running.' + GREEN + '                                        ***')
    print('*******************************************************************' + ENDC)
    raise ServiceExit

class ServiceExit(Exception):
    pass

if __name__ == '__main__':
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)

    service_start()

    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((config.get("bind_addr"), config.get("listen_port")))
        listener.listen(10)

        manager = Manager(config.get("cbscript"))
        manager.start()
        
        callback = lambda t: manager.push_template(t)
        gettemplate = GetTemplate(callback)
        gettemplate.start()

        while True:
            conn, addr = listener.accept()
            Miner(conn, manager)

    except ServiceExit:
        # Set the shutdown flag on each thread to trigger a clean shutdown
        manager.shutdown_flag.set()
        gettemplate.shutdown_flag.set()
    