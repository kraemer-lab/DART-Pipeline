from typing import Dict, Literal, NamedTuple


class PreReqInfo(NamedTuple):
    Type: Literal["Python package", "CLI executable"]
    Name: str
    Status: Literal["Installed", "Missing"]
    Info: str


class ASTValueNode(NamedTuple):
    value: str
    start_byte: int
    end_byte: int


class ASTDict(Dict):
    key: str
    value: ASTValueNode
