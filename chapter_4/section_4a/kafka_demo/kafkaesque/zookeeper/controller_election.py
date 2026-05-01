import threading, time, json
from kafkaesque.zookeeper.znodes import ELECTION_ZNODE
from kafkaesque.structs import Color

class ControllerElection:
    def __init__(self, zk, broker_id, address):
        self.zk = zk
        self.broker_id = broker_id
        self.address = address
        self.is_controller = False
        self._controller_election_thread = None
        self._standby_thread = None
        self._stop_standby_thread = threading.Event()
        self._stop_controller_thread = threading.Event()

    # =============== LIFECYCLE ===============
    def start(self):
        self.zk.ensure_path(ELECTION_ZNODE)
        self._standby_thread = threading.Thread(target=self.standby_task, daemon=True)
        self._standby_thread.start()
        identifier_payload = {"broker_id": self.broker_id, "address": self.address}
        election = self.zk.Election(ELECTION_ZNODE, identifier=json.dumps(identifier_payload))
        self._controller_election_thread = threading.Thread(target=lambda: election.run(self.controller_task), daemon=True)
        self._controller_election_thread.start()
    
    def stop(self):
        self._stop_standby_thread.set()
        self._stop_controller_thread.set()

    # =============== CONTROLLER / STANDBY ROLES ===============
    def standby_task(self):
        while not self._stop_standby_thread.is_set():
            print(f"{Color.GOLD}[♙ STANDBY BROKER] {self.broker_id} {self.address}{Color.WHITE}")
            time.sleep(3)

    def controller_task(self):
        self.is_controller = True
        self._stop_standby_thread.set()

        while not self._stop_controller_thread.is_set():
            print(f"{Color.GOLD}[♕ CONTROLLER BROKER] {self.broker_id} {self.address}{Color.WHITE}")
            time.sleep(3)
