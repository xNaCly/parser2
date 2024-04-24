# requires file benchmark.log: go test ./... -bench=Jit -run=^\$ 2>/dev/null >benchmark.log
from dataclasses import dataclass
import json
import sys

@dataclass
class Record:
    name: str
    ns_op: dict[str, float]

    def csv(self):
        floats = [self.ns_op["100000"], self.ns_op["1000000"], self.ns_op["3000000"]]
        return f"{self.name},{floats[0]},{floats[1]},{floats[2]}"

@dataclass
class PreRecord:
    name: str
    sample_size: int
    ns_op: float


def processLine(line: str) -> PreRecord:
    split = line.split(":")
    name = split[0].split("/")[1]
    type = split[1]
    rhs = split[-1].split("=")[1].strip().replace(" ", "").split("\t")
    jit_threshold = rhs[0].split("-")[0]
    ns_op = rhs[-1].split("n")[0]
    sample_size = split[2]
    if '-' in sample_size:
        sample_size = sample_size.split("-")[0]
    return PreRecord(name+"#"+type+"#"+jit_threshold, int(sample_size), float(ns_op))

def mergeRecords(l: list[PreRecord]) -> list[Record]:
    m: dict[str, Record]= {}
    for e in l:
        if e.name in m:
            m[e.name].ns_op[str(e.sample_size)] = e.ns_op
        else:
            m[e.name] = Record(e.name, {})
    return [m[k] for k in m]

with open(file="benchmark.log", mode="r", encoding="utf-8") as f:
    lines = filter(lambda l: l.startswith("Benchmark"), f.readlines())
    records = list(map(lambda l: processLine(l), lines))

print("name,100000,1000000,3000000")
print("\n".join(map(lambda l: l.csv(), mergeRecords(records))))
