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
    query_id: int = 0

@dataclass(frozen=True)
class BatchMetrics:
    send_time_ms: int
    ack_recv_time_ms: int
    record_count: int
    batch_bytes: int

@dataclass(frozen=True)
class ReadRequestMetrics:
    query_id: int
    req_id: int
    send_time_ms: float
    recv_time_ms: float
    record_count: int
    records_bytes: int

