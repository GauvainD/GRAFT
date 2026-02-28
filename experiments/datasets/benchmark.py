import csv
import itertools
import os
import shelve
import shutil
import subprocess as sp
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import neo4j

GRAFT_PATH = Path("../../")
NEO4J_URI = "neo4j://localhost"
TIMEOUT = 600
OVERWRITE = False
APPEND = False
PRUNE_TIMEOUTS = False
NUM_RUNS = 5
SAVE_TRANSFO_PATH = False

DEFAULTS: dict[str, Any] = {
    "pruning": 8,
    "minshash": None,
    "dir": "icij",
    "outputdir": "runs",
    "strat": "greedy",
    "weight": 0.5,
    "source": "sources",
    "target": "target",
    "shelve_name": "results.shelf",
    "filename": "results.csv",
    "theta": 1.0,
    "idemp": False,
}


@dataclass
class BenchmarkParams:
    strat: str
    weight: float
    dir: str
    source: str
    target: str
    pruning: int
    minshash: Optional[int]
    idemp: bool
    theta: float

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "BenchmarkParams":
        return cls(
            strat=d["strat"],
            weight=d["weight"],
            dir=d["dir"],
            source=d["source"],
            target=d["target"],
            pruning=d["pruning"],
            minshash=d["minshash"],
            idemp=d["idemp"],
            theta=d["theta"],
        )


@dataclass
class Timings:
    total_time: Optional[float]
    souffle_time: Optional[float]
    neo4j_time: Optional[float]
    sim_time: Optional[float]
    gen_time: Optional[float]
    automaton_time: Optional[float]
    num_dup: Optional[int]
    num_tot: Optional[int]

    @classmethod
    def empty(cls) -> "Timings":
        return cls(None, None, None, None, None, None, None, None)


class EvalResult:
    def __init__(self, index: str) -> None:
        self.index = index
        self.data: dict[str, list] = {}

    def __getitem__(self, name: str) -> Optional[list]:
        return self.data.get(name)

    @property
    def num_samples(self) -> int:
        if not self.data:
            return 0
        return len(next(iter(self.data.values())))

    def add_sample(self, shelf: shelve.Shelf, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            self.data.setdefault(key, []).append(value)
        shelf[self.index] = self.data


class ResultSet:
    def __init__(self) -> None:
        # dict preserves insertion order (Python 3.7+)
        self._sets: dict[str, EvalResult] = {}

    def add(self, result: EvalResult) -> None:
        self._sets[result.index] = result

    def save(self, filename: str) -> None:
        if not self._sets:
            return
        sample = next(iter(self._sets.values()))
        all_keys = list(sample.data.keys())
        if not SAVE_TRANSFO_PATH:
            all_keys = [k for k in all_keys if k != "transfo_path"]
        fieldnames = ["index"] + all_keys
        with open(filename, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for index, result in self._sets.items():
                row: dict[str, Any] = {"index": index}
                row.update({k: result.data.get(k) for k in fieldnames[1:]})
                writer.writerow(row)


def clear_data(driver: neo4j.Driver) -> None:
    driver.execute_query("MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n, r")


def get_path(driver: neo4j.Driver) -> tuple[list, list]:
    similarities: list = []
    operations: list = []
    records, _, _ = driver.execute_query(
        "MATCH ()-[p:Path]->(dst) "
        "RETURN dst.similarity AS similarity, p.operations AS operations"
    )
    for record in records:
        similarities.append(record["similarity"])
        operations.append(record["operations"])
    return similarities, operations


def get_transfo_path(driver: neo4j.Driver) -> Optional[str]:
    records, _, _ = driver.execute_query(
        """
        MATCH p = SHORTEST 1 (s:Source)-[:Meta]-*(t:Target)
        RETURN [r IN relationships(p) | r.operations]
        """
    )
    if records:
        return ":".join(";".join(t) for t in records[0][0])
    return None


def get_timings(driver: neo4j.Driver) -> Timings:
    records, _, _ = driver.execute_query(
        "MATCH (n:TIMINGS) "
        "RETURN n.total_time AS total_time, n.souffle_time AS souffle_time, "
        "n.neo4j_time AS neo4j_time, n.sim_time AS sim_time, "
        "n.gen_time AS gen_time, n.automaton_time AS automaton_time, "
        "n.num_dup AS num_dup, n.num_tot AS num_tot"
    )
    if records:
        r = records[0]
        return Timings(
            total_time=r["total_time"],
            souffle_time=r["souffle_time"],
            neo4j_time=r["neo4j_time"],
            sim_time=r["sim_time"],
            gen_time=r["gen_time"],
            automaton_time=r["automaton_time"],
            num_dup=r["num_dup"],
            num_tot=r["num_tot"],
        )
    return Timings.empty()


def build(dir_name: str) -> None:
    src = Path(os.getcwd()) / dir_name / "transfos.dl"
    dst = GRAFT_PATH / "datalog" / f"{dir_name}.dl"
    compiled = GRAFT_PATH / "datalog_compiled" / f"{dir_name}.cpp"
    try:
        shutil.copy2(src, dst)
    except OSError as exc:
        raise RuntimeError(f"Could not copy datalog file {src}: {exc}") from exc
    env = {**os.environ, "BUILD_ENABLED": "1"}
    sp.run(
        ["cargo", "build", "--release", "--bin", "transrust"],
        cwd=GRAFT_PATH,
        env=env,
        check=True,
    )
    dst.unlink()
    compiled.unlink()


def run_transrust(params: BenchmarkParams) -> bool:
    """Return True on success, False on timeout."""
    binary = GRAFT_PATH / "target" / "release" / "transrust"
    cmd = [
        str(binary),
        params.dir,
        "--neo4j",
        "--input",
        os.path.join(params.dir, f"{params.source}.pgschema"),
        "--target",
        os.path.join(params.dir, f"{params.target}.pgschema"),
        "-p",
        str(params.pruning),
        "-t",
        "1",
        "--strat",
        params.strat,
        "--weight",
        str(params.weight),
        "--theta",
        str(params.theta),
        "rule1",
    ]
    if params.minshash is not None:
        cmd += ["--minshash", str(params.minshash)]
    if params.idemp:
        cmd += ["--idempotent"]
    env = {**os.environ, "RUST_BACKTRACE": "1"}
    try:
        sp.run(cmd, timeout=TIMEOUT, env=env)
        return True
    except sp.TimeoutExpired:
        return False


def load_result_from_shelf(shelf: shelve.Shelf, index: str) -> Optional[EvalResult]:
    if index not in shelf:
        return None
    result = EvalResult(index)
    result.data = {k: list(v) for k, v in shelf[index].items()}
    if PRUNE_TIMEOUTS:
        pruned = {k: [x for x in v if x is not None] for k, v in result.data.items()}
        if any(len(v) == 0 for v in pruned.values()):
            del shelf[index]
            return None
        result.data = pruned
    return result


def eval_instance(
    index: str,
    params: BenchmarkParams,
    shelf: shelve.Shelf,
    should_build: bool,
) -> EvalResult:
    res: Optional[EvalResult] = None
    if not OVERWRITE:
        res = load_result_from_shelf(shelf, index)

    start = 0
    if res is not None:
        if not APPEND and res.num_samples >= NUM_RUNS:
            return res
        if not APPEND:
            start = res.num_samples
    else:
        res = EvalResult(index)

    with neo4j.GraphDatabase.driver(NEO4J_URI) as driver:
        for i in range(start, NUM_RUNS):
            if i == start and should_build:
                build(params.dir)
            clear_data(driver)
            print(f"INDEX {params.dir} {index} {i + 1}/{NUM_RUNS}")
            timed_out = not run_transrust(params)
            similarities, operations = get_path(driver)
            timings = get_timings(driver)
            full_path = get_transfo_path(driver)
            print(operations)
            similarity: Optional[float] = None
            len_ops: Optional[int] = None
            if not timed_out and similarities and operations:
                similarity = similarities[0]
                len_ops = len(operations[0])
            res.add_sample(
                shelf,
                similarity=similarity,
                path=len_ops,
                time=timings.total_time,
                souffle_time=timings.souffle_time,
                neo4j_time=timings.neo4j_time,
                sim_time=timings.sim_time,
                gen_time=timings.gen_time,
                automaton_time=timings.automaton_time,
                num_dup=timings.num_dup,
                num_tot=timings.num_tot,
                transfo_path=full_path,
            )
    return res


def eval_instances(
    shelf: shelve.Shelf,
    defaults: dict[str, Any],
    *params: tuple,
) -> ResultSet:
    names = [v[0] for v in params]
    values = [v[1] for v in params]
    previous_dir = ""
    results = ResultSet()
    for item in itertools.product(*values):
        inputs = defaults.copy()
        index_parts: list[str] = []
        for name, value in zip(names, item):
            if isinstance(name, tuple):
                for n, v in zip(name, value):
                    inputs[n] = v
                    index_parts.append(str(v))
            else:
                inputs[name] = value
                index_parts.append(str(value))
        bench_params = BenchmarkParams.from_dict(inputs)
        should_build = bench_params.dir != previous_dir
        if should_build:
            previous_dir = bench_params.dir
        index = "-".join(index_parts)
        print(index, "(build)" if should_build else "")
        results.add(eval_instance(index, bench_params, shelf, should_build))
    return results


def run_benchmark(defaults: dict[str, Any], *params: tuple) -> None:
    output_dir = defaults["outputdir"]
    shelf_path = f"{output_dir}/{defaults['shelve_name']}"
    csv_path = f"{output_dir}/{defaults['filename']}"
    with shelve.open(shelf_path, writeback=True) as shelf:
        results = eval_instances(shelf, defaults, *params)
        results.save(csv_path)


if __name__ == "__main__":
    run_benchmark(
        DEFAULTS,
        (
            "dir",
            [
                "dblp_to_amalgam1",
                "amalgam1_to_amalgam3",
                "persondata",
                "flighthotel",
            ],
        ),
        ("strat", ["greedy", "random", "weighted_distance", "naive"]),
        ("minshash", [10, 50, 100, 200, None]),
    )
