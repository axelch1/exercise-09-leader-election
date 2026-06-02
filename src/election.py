import logging
import threading
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_node_id: Optional[int] = None
_peers: list[str] = []
_own_url: str = ""
_leader_id: Optional[int] = None
_leader_url: Optional[str] = None
_election_in_progress = False


def configure(node_id: int, peers: list[str], own_url: str):
    global _node_id, _peers, _own_url
    _node_id = node_id
    _peers = peers
    _own_url = own_url


def start_election():
    global _election_in_progress

    if _election_in_progress:
        logger.info("Election already in progress")
        return

    _election_in_progress = True
    logger.info("Node %s starting election", _node_id)

    ok_received = False
    for peer_url in _peers:
        try:
            resp = requests.post(
                f"{peer_url}/api/election/message",
                json={"sender_id": _node_id},
                timeout=3,
            )
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                ok_received = True
        except requests.RequestException:
            logger.warning("Could not reach peer %s", peer_url)

    if not ok_received:
        logger.info("No OK from any peer, declaring victory")
        declare_victory()


def handle_election_message(sender_id: int) -> bool:
    if sender_id < _node_id:
        if not _election_in_progress:
            thread = threading.Thread(target=start_election, daemon=True)
            thread.start()
        return True
    return False


def declare_victory():
    global _leader_id, _leader_url, _election_in_progress

    _leader_id = _node_id
    _leader_url = _own_url
    _election_in_progress = False
    logger.info("Node %s declares itself as leader", _node_id)

    for peer_url in _peers:
        try:
            requests.post(
                f"{peer_url}/api/election/coordinator",
                json={"leader_id": _node_id, "leader_url": _own_url},
                timeout=3,
            )
        except requests.RequestException:
            logger.warning("Could not notify peer %s", peer_url)


def set_leader(leader_id: int, leader_url: str):
    global _leader_id, _leader_url, _election_in_progress
    _leader_id = leader_id
    _leader_url = leader_url
    _election_in_progress = False
    logger.info("Node %s acknowledges leader %s", _node_id, leader_id)


def heartbeat_check():
    def _check():
        global _leader_id, _leader_url
        while True:
            time.sleep(5)
            if _leader_id is None or _leader_id == _node_id:
                continue
            try:
                resp = requests.get(f"{_leader_url}/health", timeout=3)
                if resp.status_code != 200:
                    raise Exception("Leader unhealthy")
            except requests.RequestException:
                logger.warning("Leader %s down, starting election", _leader_id)
                _leader_id = None
                _leader_url = None
                start_election()

    thread = threading.Thread(target=_check, daemon=True)
    thread.start()
