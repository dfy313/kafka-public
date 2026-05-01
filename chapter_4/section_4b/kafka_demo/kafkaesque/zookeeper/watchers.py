import json
from kafkaesque.zookeeper.znodes import CONTROLLER_ZNODE

def install_controller_watch(zk, controller_view):
    @zk.DataWatch(CONTROLLER_ZNODE)
    def _controller_watch(data, _stat, _event=None):
        if not data:
            controller_view["broker_id"] = None
            controller_view["address"] = None
            return
        current_controller = json.loads(data.decode("utf-8"))
        controller_view["broker_id"] = current_controller.get("broker_id")
        controller_view["address"]   = current_controller.get("address")
