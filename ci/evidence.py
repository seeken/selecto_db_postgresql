#!/usr/bin/env python3
"""Export only bounded counts and fixed failure labels from guest-local output."""
import hashlib
import json
import pathlib
import re
import os
import sys

STAGES = {"initialize", "checkout", "dependencies", "precommit", "docs", "dialyzer", "package", "live", "verify_sql", "diagnostics"}
PROOFS = {
    "selecto_db_postgresql.adapter_safety.v1": (48, 16, 3),
    "selecto_db_postgresql.transaction_protocol.v1": (114, 38, 3),
    "selecto_db_postgresql.stream_protocol.v2": (840, 120, 7),
    "selecto_db_postgresql.pool_protocol.v1": (78, 26, 3),
}


def test_counts(text):
    text = re.sub(r"\x1b\[[0-9;]*m", "", text)
    current = re.findall(r"^Result: (\d+) passed(?:, (\d+) failed)?(?:, (\d+) skipped)?(?:, (\d+) excluded)?$", text, re.M)
    parenthesized = re.findall(r"^(\d+) tests, (\d+) failures(?:, (\d+) skipped)? \((\d+) excluded\)$", text, re.M)
    traditional = re.findall(r"^(\d+) tests, (\d+) failures(?:, (\d+) skipped)?$", text, re.M)
    if len(current) + len(parenthesized) + len(traditional) != 1:
        raise ValueError("one complete test summary required")
    if current:
        passed, failures, skipped, excluded = (int(n or 0) for n in current[0])
        total = passed + failures + skipped + excluded
    elif parenthesized:
        executed, failures, skipped, excluded = (int(n or 0) for n in parenthesized[0])
        total = executed + excluded
        passed = executed - failures - skipped
    else:
        total, failures, skipped = (int(n or 0) for n in traditional[0])
        excluded = 0
        passed = total - failures - skipped
    return dict(total=total, passed=passed, failures=failures, skipped=skipped, excluded=excluded)


def summary(stage, text, auxiliary=None):
    if stage not in STAGES:
        raise ValueError("unknown CI stage")
    result = dict(schema="selecto.postgresql-ci-stage.v1", stage=stage, status="passed", exit_code=0)
    if stage == "initialize":
        refs = re.findall(r"^[0-9a-f]{40}$", text, re.M)
        if len(refs) != 1:
            raise ValueError("one immutable Core ref required")
        result["core_ref"] = refs[0]
    if stage in {"precommit", "live"}:
        counts = test_counts(text)
        minimum = 104 if stage == "precommit" else 173
        if counts["total"] < 173 or counts["passed"] < minimum or counts["failures"] != 0 or counts["skipped"] != 0 or (stage == "live" and counts["excluded"] != 0):
            raise ValueError("required test coverage differs")
        result["tests"] = counts
    if stage == "precommit":
        rows = re.findall(r"^PROVED ([a-z0-9._]+): (\d+) checks \((\d+) states x (\d+) invariants, proof=bounded_exhaustive\)$", text, re.M)
        observed = {name: tuple(map(int, values)) for name, *values in rows}
        if len(rows) != 4 or observed != PROOFS or "No cycles found" not in text:
            raise ValueError("complete bounded proofs and cycle gate required")
        result["bounded_proofs"] = [{"model": name, "checks": values[0], "states": values[1], "invariants": values[2]} for name, values in observed.items()]
        result["compile_cycles"] = 0
    if stage == "dialyzer":
        rows = re.findall(r"Total errors: (\d+), Skipped: (\d+), Unnecessary Skips: (\d+)", text)
        if rows != [("0", "0", "0")]:
            raise ValueError("clean Dialyzer result required")
        result["dialyzer"] = dict(errors=0, skipped=0, unnecessary_skips=0)
    if stage == "verify_sql":
        expected = {
            "format": "selecto.formal_verification", "format_version": 1,
            "proof_level": "bounded_live_differential",
            "model": "selecto_db_postgresql.relational_semantics.v1",
            "state_count": 232, "invariant_count": 1, "check_count": 232,
            "proved?": True, "counterexamples": [],
        }
        valid_report = isinstance(auxiliary, dict) and auxiliary.keys() == expected.keys() and all(type(auxiliary[key]) is type(value) and auxiliary[key] == value for key, value in expected.items())
        if not valid_report or re.findall(r"^PROVED selecto_db_postgresql.relational_semantics.v1: (\d+) live differential checks \(proof=bounded_live_differential\)$", text, re.M) != ["232"]:
            raise ValueError("complete live differential proof required")
        result["relational_semantics"] = auxiliary
    if stage == "package":
        package = pathlib.Path(auxiliary)
        result["package"] = {"filename": package.name, "sha256": hashlib.sha256(package.read_bytes()).hexdigest(), "mode": "metadata_assembly"}
    return result


def failure(stage, code, phase, text):
    if stage not in STAGES or phase not in {"command", "evidence_validation"}:
        raise ValueError("unknown failure stage")
    classification = "evidence_validation" if phase == "evidence_validation" else "unclassified_tool_failure"
    if phase == "command":
        lowered = text.lower()
        categories = (
            ("git_revision_unavailable", ("not our ref", "reference is not a tree", "unable to read tree")),
            ("git_authentication", ("authentication failed", "could not read username")),
            ("dependency_resolution", ("version solving failed", "failed to resolve")),
            ("dependency_compilation", ("could not compile dependency", "compilation error")),
            ("source_provenance", ("source provenance failed", "immutable core declaration")),
            ("network_transport", ("connection reset", "could not resolve host", "connection timed out")),
            ("test_failure", (" failure", " failed")),
        )
        for label, needles in categories:
            if any(needle in lowered for needle in needles):
                classification = label
                break
    return dict(schema="selecto.postgresql-ci-stage.v1", stage=stage, status="failed", exit_code=int(code), phase=phase, classification=classification)


if __name__ == "__main__":
    mode, stage, raw_path, output_path, *arguments = sys.argv[1:]
    text = pathlib.Path(raw_path).read_bytes()[-4194304:].decode("utf-8", errors="replace")
    if mode == "failure":
        result = failure(stage, arguments[0], arguments[1], text)
    elif mode == "summary":
        auxiliary = json.loads(pathlib.Path(arguments[0]).read_text()) if stage == "verify_sql" else arguments[0] if arguments else None
        result = summary(stage, text, auxiliary)
    else:
        raise ValueError("unknown evidence command")
    pathlib.Path(output_path).write_text(json.dumps(result, indent=2) + "\n")
    if mode == "summary" and stage == "initialize" and os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            output.write("ref=" + result["core_ref"] + "\n")
    print("PostgreSQL CI " + stage + ": " + result["status"])
