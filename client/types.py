from dataclasses import dataclass

@dataclass(frozen=True)
class Node:
    cluster_name: str
    id: int
    host: str
    port: int

@dataclass(frozen=True)
class Record:
    key_id: int
    timestamp: int
    content: any

@dataclass(frozen=True)
class QueryRequest:
    min_id: int
    min_ts: int
    max_id: int
    max_ts: int
