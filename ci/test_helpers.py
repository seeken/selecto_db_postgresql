import json
import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

import evidence
import git_credential_readonly


class Boundaries(unittest.TestCase):
    def test_credentials_are_get_only_exact_core_https_and_selected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = pathlib.Path(folder)
            root.joinpath("siblings").write_text("seeken/selecto\n")
            root.joinpath("token").write_text("disposable-read-only")
            request = {"protocol": "https", "host": "github.com", "path": "seeken/selecto.git"}
            self.assertIn("password=disposable-read-only", git_credential_readonly.credential("get", request, root))
            for operation, change in [
                ("store", {}), ("erase", {}), ("get", {"host": "other.example"}),
                ("get", {"protocol": "http"}), ("get", {"path": "seeken/other.git"}),
            ]:
                self.assertEqual("", git_credential_readonly.credential(operation, dict(request, **change), root))
            root.joinpath("siblings").write_text("seeken/other\n")
            self.assertEqual("", git_credential_readonly.credential("get", request, root))

    def test_coverage_parser_refuses_failures_skips_and_partial_live_runs(self):
        self.assertEqual(173, evidence.summary("live", "Result: 173 passed\n")["tests"]["passed"])
        self.assertEqual(173, evidence.summary("live", "173 tests, 0 failures\n")["tests"]["passed"])
        self.assertEqual(174, evidence.summary("live", "Result: 174 passed\n")["tests"]["passed"])
        for text in ["Result: 172 passed\n", "Result: 173 passed, 1 skipped\n",
                     "173 tests, 1 failures\n", "Result: 173 passed, 1 excluded\n", ""]:
            with self.assertRaises(ValueError):
                evidence.summary("live", text)

    def test_default_test_exclusions_require_all_four_independent_proofs(self):
        proofs = "\n".join(f"PROVED {name}: {n} checks ({states} states x {invariants} invariants, proof=bounded_exhaustive)" for name, (n, states, invariants) in evidence.PROOFS.items())
        text = "Result: 104 passed, 69 excluded\n" + proofs + "\nNo cycles found\n"
        self.assertEqual(4, len(evidence.summary("precommit", text)["bounded_proofs"]))
        actual_traditional = text.replace("Result: 104 passed, 69 excluded", "104 tests, 0 failures (69 excluded)")
        self.assertEqual(dict(total=173, passed=104, failures=0, skipped=0, excluded=69), evidence.summary("precommit", actual_traditional)["tests"])
        with self.assertRaises(ValueError):
            evidence.summary("live", "173 tests, 0 failures (1 excluded)\n")
        with self.assertRaises(ValueError):
            evidence.summary("precommit", text.replace("Result: 104 passed, 69 excluded", "173 tests, 0 failures, 69 excluded"))
        for incomplete in [text.replace("No cycles found", ""), text.replace("840 checks", "839 checks"), text.replace("104 passed", "103 passed")]:
            with self.assertRaises(ValueError):
                evidence.summary("precommit", incomplete)

    def test_nonzero_stage_preserves_exit_and_exports_no_raw_error(self):
        with tempfile.TemporaryDirectory() as folder:
            root = pathlib.Path(folder)
            tools = root / "bin"
            tools.mkdir()
            mix = tools / "mix"
            mix.write_text("#!/bin/sh\nprintf 'token=never-export postgres://user:password@host/db failure\\n'\nexit 8\n")
            mix.chmod(0o700)
            output = root / "reports"
            output.mkdir()
            output.joinpath("dependencies.json").write_text('{"status":"passed"}')
            env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ["PATH"])
            run = pathlib.Path(__file__).resolve().parent / "run"
            result = subprocess.run([str(run), "dependencies", str(output)], env=env, capture_output=True)
            self.assertEqual(8, result.returncode)
            report = json.loads(output.joinpath("dependencies.json").read_text())
            self.assertEqual("failed", report["status"])
            self.assertEqual(8, report["exit_code"])
            self.assertNotIn("never-export", json.dumps(report))
            self.assertNotIn("postgres://", json.dumps(report))

    def test_package_output_is_fresh_unique_and_version_independent(self):
        helpers = pathlib.Path(__file__).resolve().parent
        for outputs, expected in [
            (["selecto_db_postgresql-9.9.0.tar"], 0),
            ([], 1),
            (["selecto_db_postgresql-9.9.0.tar", "selecto_db_postgresql-9.9.1.tar"], 1),
        ]:
            with self.subTest(outputs=outputs), tempfile.TemporaryDirectory() as folder:
                root = pathlib.Path(folder)
                workspace = root / "workspace"
                workspace.joinpath("ci").mkdir(parents=True)
                for name in ["run", "evidence.py"]:
                    shutil.copy2(helpers / name, workspace / "ci" / name)
                workspace.joinpath("selecto_db_postgresql-0.5.0.tar").write_text("stale")
                tools = root / "bin"
                tools.mkdir()
                mix = tools / "mix"
                mix.write_text("#!/bin/sh\n" + "\n".join(f"printf fixture > {name}" for name in outputs) + "\nexit 0\n")
                mix.chmod(0o700)
                env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ["PATH"])
                result = subprocess.run([str(workspace / "ci" / "run"), "package", str(root / "reports")], env=env, capture_output=True)
                self.assertEqual(expected, result.returncode)
                self.assertFalse(workspace.joinpath("selecto_db_postgresql-0.5.0.tar").exists())
                report = json.loads(root.joinpath("reports/package.json").read_text())
                if expected == 0:
                    self.assertEqual("selecto_db_postgresql-9.9.0.tar", report["package"]["filename"])
                else:
                    self.assertEqual("failed", report["status"])

    def test_default_clone_fetches_the_declared_older_commit(self):
        with tempfile.TemporaryDirectory() as folder:
            root = pathlib.Path(folder)
            source = root / "source"
            source.mkdir()
            def git(*args):
                return subprocess.check_output(["git", "-C", str(source), *args], text=True).strip()
            git("init", "--quiet", "--initial-branch=main")
            git("config", "user.name", "CI fixture")
            git("config", "user.email", "ci@example.invalid")
            source.joinpath("value").write_text("declared")
            git("add", "value")
            git("commit", "--quiet", "-m", "declared")
            declared = git("rev-parse", "HEAD")
            source.joinpath("value").write_text("new default")
            git("commit", "--quiet", "-am", "default moved")
            default = git("rev-parse", "HEAD")
            workspace = root / "workspace"
            workspace.joinpath("ci").mkdir(parents=True)
            helpers = pathlib.Path(__file__).resolve().parent
            for name in ["checkout-core", "core_ref.exs", "source_refs.exs"]:
                shutil.copy2(helpers / name, workspace / "ci" / name)
            workspace.joinpath("mix.exs").write_text(f'defmodule Project do\n@selecto_ref "{declared}"\nend')
            workspace.joinpath("mix.lock").write_text(f'%{{selecto: {{:git, "https://github.com/seeken/selecto.git", "{declared}", [ref: "{declared}"]}}}}')
            tools = root / "bin"
            tools.mkdir()
            checkout = tools / "selecto-ci-checkout"
            checkout.write_text('#!/bin/sh\nset -eu\ntest "$#" = 2\ntest "$1" = seeken/selecto\ngit clone --quiet --no-tags --depth 1 "$FIXTURE_CORE_URL" "$2"\n')
            checkout.chmod(0o700)
            env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ["PATH"], FIXTURE_CORE_URL=source.as_uri())
            subprocess.run([str(workspace / "ci" / "checkout-core")], env=env, capture_output=True, check=True)
            actual = subprocess.check_output(["git", "-C", str(workspace / "selecto"), "rev-parse", "HEAD"], text=True).strip()
            self.assertEqual(declared, actual)
            self.assertNotEqual(default, actual)


if __name__ == "__main__":
    unittest.main()
