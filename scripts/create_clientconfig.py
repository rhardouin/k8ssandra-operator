#!/usr/bin/env python3
"""Create the e2e ClientConfig without platform-specific getopt dependencies."""

import argparse
import base64
import json
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class Arguments:
    src_kubeconfig: Optional[str]
    dest_kubeconfig: Optional[str]
    src_context: str
    dest_context: str
    namespace: Optional[str]
    serviceaccount: str
    output_dir: Optional[str]


def run_kubectl(arguments: list[str], input_bytes: Optional[bytes] = None) -> bytes:
    command = ["kubectl", *arguments]
    try:
        return subprocess.run(command, input=input_bytes, check=True, capture_output=True).stdout
    except subprocess.CalledProcessError as error:
        stderr = error.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"kubectl command failed: {' '.join(command)}: {stderr}") from error


def kubectl_json(kubeconfig: Optional[str], context: str, arguments: list[str]) -> dict:
    options = ["--context", context, *arguments]
    if kubeconfig:
        options[:0] = ["--kubeconfig", kubeconfig]
    output = run_kubectl(options)
    return json.loads(output)


def source_credentials(arguments: Arguments) -> tuple[str, str]:
    namespace = ["-n", arguments.namespace] if arguments.namespace else []
    service_account = kubectl_json(arguments.src_kubeconfig, arguments.src_context, [
        *namespace, "get", "serviceaccount", arguments.serviceaccount, "-o", "json",
    ])
    secrets = service_account.get("secrets", [])
    if not secrets:
        raise RuntimeError(f"service account {arguments.serviceaccount} has no token Secret")
    secret = kubectl_json(arguments.src_kubeconfig, arguments.src_context, [
        *namespace, "get", "secret", secrets[0]["name"], "-o", "json",
    ])
    try:
        token = base64.b64decode(secret["data"]["token"], validate=True).decode("utf-8")
        ca_certificate = secret["data"]["ca.crt"]
    except (KeyError, ValueError, UnicodeDecodeError) as error:
        raise RuntimeError("service account token Secret has invalid token or ca.crt data") from error
    return token, ca_certificate


def source_cluster(arguments: Arguments) -> tuple[str, str]:
    config_arguments = ["config", "view", "--raw", "-o", "json"]
    if arguments.src_kubeconfig:
        config_arguments[:0] = ["--kubeconfig", arguments.src_kubeconfig]
    config = json.loads(run_kubectl(config_arguments))
    contexts = {item["name"]: item["context"] for item in config.get("contexts", [])}
    clusters = {item["name"]: item["cluster"] for item in config.get("clusters", [])}
    try:
        cluster_name = contexts[arguments.src_context]["cluster"]
        server = clusters[cluster_name]["server"]
    except KeyError as error:
        raise RuntimeError(f"source context {arguments.src_context} is incomplete") from error
    if "127.0.0.1" in server:
        pods = kubectl_json(arguments.src_kubeconfig, arguments.src_context, [
            "-n", "kube-system", "get", "pod", "-l", "component=kube-apiserver", "-o", "json",
        ])
        try:
            server = f"https://{pods['items'][0]['status']['podIP']}:6443"
        except (KeyError, IndexError) as error:
            raise RuntimeError("source kube-apiserver Pod IP is unavailable") from error
    return cluster_name, server


def render_kubeconfig(context: str, cluster: str, server: str, ca_certificate: str, token: str,
                      serviceaccount: str) -> bytes:
    user = f"{cluster}-{serviceaccount}"
    return (
        "apiVersion: v1\nclusters:\n- cluster:\n"
        f"    certificate-authority-data: {ca_certificate}\n    server: {server}\n"
        f"  name: {cluster}\ncontexts:\n- context:\n    cluster: {cluster}\n    user: {user}\n"
        f"  name: {context}\ncurrent-context: {context}\nkind: Config\npreferences: {{}}\nusers:\n"
        f"- name: {user}\n  user:\n    token: {token}\n"
    ).encode("utf-8")


def apply_client_config(arguments: Arguments, kubeconfig_path: Path, client_config_path: Path) -> None:
    secret_name = f"{arguments.src_context}-config".replace("_", "-")
    destination = ["--context", arguments.dest_context]
    if arguments.dest_kubeconfig:
        destination[:0] = ["--kubeconfig", arguments.dest_kubeconfig]
    if arguments.namespace:
        destination.extend(["-n", arguments.namespace])
    try:
        run_kubectl([*destination, "delete", "secret", secret_name])
    except RuntimeError:
        pass
    run_kubectl([*destination, "create", "secret", "generic", secret_name,
                 f"--from-file={kubeconfig_path}"])
    run_kubectl([*destination, "apply", "-f", str(client_config_path)])


def write_artifacts(arguments: Arguments, kubeconfig: bytes) -> tuple[Path, Path]:
    output_directory = Path(arguments.output_dir or tempfile.mkdtemp())
    output_directory.mkdir(parents=True, exist_ok=True)
    kubeconfig_path = output_directory / "kubeconfig"
    kubeconfig_path.write_bytes(kubeconfig)
    client_config_name = arguments.src_context.replace("_", "-")
    secret_name = f"{arguments.src_context}-config".replace("_", "-")
    client_config_path = output_directory / f"{client_config_name}.yaml"
    client_config_path.write_text(
        "apiVersion: config.k8ssandra.io/v1beta1\nkind: ClientConfig\nmetadata:\n"
        f"  name: {client_config_name}\nspec:\n"
        f"  contextName: {arguments.src_context}\n  kubeConfigSecret:\n"
        f"    name: {secret_name}\n",
        encoding="utf-8",
    )
    return kubeconfig_path, client_config_path


def current_context(kubeconfig: Optional[str]) -> str:
    arguments = ["config", "current-context"]
    if kubeconfig:
        arguments[:0] = ["--kubeconfig", kubeconfig]
    return run_kubectl(arguments).decode("utf-8").strip()


def parse_arguments() -> Arguments:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-kubeconfig")
    parser.add_argument("--dest-kubeconfig")
    parser.add_argument("--src-context")
    parser.add_argument("--dest-context")
    parser.add_argument("--namespace")
    parser.add_argument("--serviceaccount", default="k8ssandra-operator")
    parser.add_argument("--output-dir")
    parsed = parser.parse_args()
    if not parsed.src_context and not parsed.src_kubeconfig:
        print("At least one of the --src-context or --src-kubeconfig options must be specified")
        raise SystemExit(1)
    src_kubeconfig = parsed.src_kubeconfig
    dest_kubeconfig = parsed.dest_kubeconfig
    src_context = parsed.src_context or current_context(src_kubeconfig)
    dest_context = parsed.dest_context or current_context(dest_kubeconfig)
    return Arguments(
        src_kubeconfig=src_kubeconfig,
        dest_kubeconfig=dest_kubeconfig,
        src_context=src_context,
        dest_context=dest_context,
        namespace=parsed.namespace,
        serviceaccount=parsed.serviceaccount,
        output_dir=parsed.output_dir,
    )


def main() -> None:
    arguments = parse_arguments()
    token, ca_certificate = source_credentials(arguments)
    cluster, server = source_cluster(arguments)
    kubeconfig = render_kubeconfig(arguments.src_context, cluster, server, ca_certificate, token,
                                   arguments.serviceaccount)
    kubeconfig_path, client_config_path = write_artifacts(arguments, kubeconfig)
    apply_client_config(arguments, kubeconfig_path, client_config_path)


if __name__ == "__main__":
    main()
