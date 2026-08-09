import json
import os
import pathlib
import subprocess
import tempfile
import unittest

from create_clientconfig import render_kubeconfig


class RenderKubeconfigTest(unittest.TestCase):
    def test_renders_the_client_kubeconfig(self) -> None:
        rendered = render_kubeconfig("kind-test", "kind-test", "https://10.0.0.1:6443", "Y2E=", "token", "operator")
        expected = b"""apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: Y2E=
    server: https://10.0.0.1:6443
  name: kind-test
contexts:
- context:
    cluster: kind-test
    user: kind-test-operator
  name: kind-test
current-context: kind-test
kind: Config
preferences: {}
users:
- name: kind-test-operator
  user:
    token: token
"""
        self.assertEqual(expected, rendered)


class CreateClientConfigMakeTargetTest(unittest.TestCase):
    def test_invokes_the_python_clientconfig_helper(self) -> None:
        repository = pathlib.Path(__file__).resolve().parent.parent

        result = subprocess.run(
            ["make", "-n", "NUM_CLUSTERS=1", "NS=test-namespace",
             "KIND_KUBECONFIG=/tmp/kind-kubeconfig", "create-clientconfig"],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertIn("python3 scripts/create_clientconfig.py", result.stdout)
        self.assertNotIn("scripts/create-clientconfig.sh", result.stdout)


class CreateClientConfigCliCompatibilityTest(unittest.TestCase):
    def run_clientconfig(self, arguments: list[str]) -> tuple[list[dict], dict[str, bytes]]:
        script = pathlib.Path(__file__).with_name("create_clientconfig.py")
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = pathlib.Path(temporary_directory)
            bin_directory = temporary / "bin"
            bin_directory.mkdir()
            output_directory = temporary / "output"
            log_path = temporary / "kubectl-log.jsonl"
            fake_kubectl = temporary / "fake_kubectl.py"
            fake_kubectl.write_text("""import json
import os
import sys

arguments = sys.argv[1:]
observed_kubeconfig = os.environ.get("KUBECONFIG")
with open(os.environ[\"CLIENTCONFIG_KUBECTL_LOG\"], \"a\") as log:
    log.write(json.dumps({\"arguments\": arguments, \"kubeconfig\": observed_kubeconfig, \"stdin\": sys.stdin.read()}) + \"\\n\")
if arguments[-2:] == [\"config\", \"current-context\"]:
    print(\"source-from-kubeconfig\" if \"/source/kubeconfig\" in arguments else \"default-context\")
elif \"config\" in arguments and \"view\" in arguments:
    print(json.dumps({\"contexts\": [
        {\"name\": \"source-from-kubeconfig\", \"context\": {\"cluster\": \"source-cluster\"}},
        {\"name\": \"source_context\", \"context\": {\"cluster\": \"source-cluster\"}},
        {\"name\": \"default-context\", \"context\": {\"cluster\": \"source-cluster\"}},
    ], \"clusters\": [{\"name\": \"source-cluster\", \"cluster\": {\"server\": \"https://10.0.0.1:6443\"}}]}))
elif \"serviceaccount\" in arguments:
    print(json.dumps({\"secrets\": [{\"name\": \"operator-token\"}]}))
elif \"secret\" in arguments:
    print(json.dumps({\"data\": {\"token\": \"dG9rZW4=\", \"ca.crt\": \"Y2E=\"}}))
""")
            kubectl = bin_directory / "kubectl"
            kubectl.write_text("#!/bin/sh\nexec \"$CLIENTCONFIG_TEST_PYTHON\" \"$CLIENTCONFIG_FAKE_KUBECTL\" \"$@\"\n")
            kubectl.chmod(0o755)
            environment = os.environ | {
                "CLIENTCONFIG_FAKE_KUBECTL": str(fake_kubectl),
                "CLIENTCONFIG_KUBECTL_LOG": str(log_path),
                "CLIENTCONFIG_TEST_PYTHON": os.sys.executable,
                "HOME": str(temporary / "home"),
                "KUBECONFIG": "/controlled/kubeconfig",
                "PATH": f"{bin_directory}:{os.environ['PATH']}",
            }
            result = subprocess.run(
                [os.sys.executable, str(script), *arguments, "--output-dir", str(output_directory)],
                check=True,
                capture_output=True,
                text=True,
                env=environment,
            )
            self.assertEqual("", result.stderr)
            calls = [json.loads(line) for line in log_path.read_text().splitlines()]
            artifacts = {artifact.name: artifact.read_bytes() for artifact in output_directory.iterdir()}
            return calls, artifacts

    def test_requires_a_source_context_or_kubeconfig(self) -> None:
        script = pathlib.Path(__file__).with_name("create_clientconfig.py")

        result = subprocess.run(
            [os.sys.executable, str(script)],
            capture_output=True,
            text=True,
        )

        self.assertEqual(1, result.returncode)
        self.assertIn("At least one of the --src-context or --src-kubeconfig options must be specified", result.stdout)

    def test_uses_source_kubeconfig_context_and_destination_defaults(self) -> None:
        calls, artifacts = self.run_clientconfig([
            "--src-kubeconfig", "/source/kubeconfig", "--namespace", "test-namespace",
        ])
        commands = [call["arguments"] for call in calls]

        self.assertIn(["--kubeconfig", "/source/kubeconfig", "config", "current-context"], commands)
        self.assertTrue(any(command[:6] == [
            "--context", "default-context", "-n", "test-namespace", "apply", "-f",
        ] and command[-1].endswith("/source-from-kubeconfig.yaml") for command in commands))
        self.assertTrue(all(call["kubeconfig"] == "/controlled/kubeconfig" for call in calls))
        self.assertIn("kubeconfig", artifacts)
        client_config = artifacts["source-from-kubeconfig.yaml"].decode("utf-8")
        self.assertIn("name: source-from-kubeconfig", client_config)
        self.assertIn("contextName: source-from-kubeconfig", client_config)
        self.assertNotIn("namespace: test-namespace", client_config)

    def test_uses_kubeconfig_environment_with_an_explicit_source_context(self) -> None:
        calls, artifacts = self.run_clientconfig(["--src-context", "source_context"])
        commands = [call["arguments"] for call in calls]

        self.assertIn([
            "--context", "source_context",
            "get", "serviceaccount", "k8ssandra-operator", "-o", "json",
        ], commands)
        self.assertTrue(all("--kubeconfig" not in call["arguments"] for call in calls))
        self.assertTrue(all(call["kubeconfig"] == "/controlled/kubeconfig" for call in calls))
        client_config = artifacts["source-context.yaml"].decode("utf-8")
        self.assertIn("contextName: source_context", client_config)

    def test_replaces_an_existing_secret_before_applying_client_config(self) -> None:
        calls, _ = self.run_clientconfig([
            "--src-kubeconfig", "/source/kubeconfig", "--namespace", "test-namespace",
        ])
        commands = [call["arguments"] for call in calls]
        delete_index = commands.index([
            "--context", "default-context", "-n", "test-namespace", "delete", "secret",
            "source-from-kubeconfig-config",
        ])
        create_index = next(index for index, command in enumerate(commands) if command[:8] == [
            "--context", "default-context", "-n", "test-namespace", "create", "secret", "generic",
            "source-from-kubeconfig-config",
        ])
        self.assertTrue(commands[create_index][-1].endswith("/kubeconfig"))
        apply_index = next(index for index, command in enumerate(commands)
                           if command[-2] == "-f" and command[-1].endswith(".yaml"))

        self.assertLess(delete_index, create_index)
        self.assertLess(create_index, apply_index)


if __name__ == "__main__":
    unittest.main()
