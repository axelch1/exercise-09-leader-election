import os
from datetime import datetime, timezone
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session
from src.database import Base, engine, get_db
from src.models import Node
from src.schemas import NodeCreate, NodeResponse, NodeUpdate
from src import election

Base.metadata.create_all(bind=engine)
app = FastAPI()


class ElectionMessage(BaseModel):
    sender_id: int


class CoordinatorMessage(BaseModel):
    leader_id: int
    leader_url: str


@app.on_event("startup")
def startup():
    import threading
    node_id = int(os.environ["NODE_ID"])
    peers_raw = os.environ.get("PEERS", "")
    peers = [p for p in peers_raw.split(",") if p] if peers_raw else []
    hostname = os.environ.get("HOSTNAME", "localhost")
    own_url = f"http://{hostname}:8080"
    election.configure(node_id, peers, own_url)
    election.heartbeat_check()

    def delayed_election():
        import time
        for attempt in range(5):
            time.sleep(1)
            if election._leader_id is not None:
                return
            election.start_election()

    threading.Thread(target=delayed_election, daemon=True).start()


@app.get("/api/election/id")
def get_node_id():
    return {"node_id": election._node_id}


@app.get("/leader")
@app.get("/api/election/leader")
def get_leader():
    return {
        "leader_id": election._leader_id,
        "leader_url": election._leader_url,
    }


@app.post("/api/election/message")
def receive_election(msg: ElectionMessage):
    ok = election.handle_election_message(msg.sender_id)
    if ok:
        return {"status": "ok"}
    return {"status": "ignored"}


@app.post("/api/election/coordinator")
def receive_coordinator(msg: CoordinatorMessage):
    election.set_leader(msg.leader_id, msg.leader_url)
    return {"status": "ok"}


@app.post("/api/election/start")
def trigger_election():
    election.start_election()
    return {"status": "election_started"}

@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        db_status = "disconnected"
    count = db.query(Node).filter(Node.status == "active").count()
    return {"status": "ok", "db": db_status, "nodes_count": count}

@app.post("/api/nodes", response_model=NodeResponse, status_code=201)
def register_node(node: NodeCreate, db: Session = Depends(get_db)):
    existing = db.query(Node).filter(Node.name == node.name).first()
    if existing:
        raise HTTPException(status_code=409, detail="Node already exists")
    db_node = Node(name=node.name, host=node.host, port=node.port)
    db.add(db_node)
    db.commit()
    db.refresh(db_node)
    return db_node

@app.get("/api/nodes", response_model=list[NodeResponse])
def list_nodes(db: Session = Depends(get_db)):
    return db.query(Node).all()

@app.get("/api/nodes/{name}", response_model=NodeResponse)
def get_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    return node

@app.put("/api/nodes/{name}", response_model=NodeResponse)
def update_node(name: str, update: NodeUpdate, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    if update.host is not None:
        node.host = update.host
    if update.port is not None:
        node.port = update.port
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(node)
    return node

@app.delete("/api/nodes/{name}", status_code=204)
def delete_node(name: str, db: Session = Depends(get_db)):
    node = db.query(Node).filter(Node.name == name).first()
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.status = "inactive"
    node.updated_at = datetime.now(timezone.utc)
    db.commit()
    return Response(status_code=204)
