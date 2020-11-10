from dataclasses import dataclass

@dataclass
class Metric:
    status: int
    response_time: int
    host: str
    timestamp: float
