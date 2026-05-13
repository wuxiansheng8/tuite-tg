from __future__ import annotations

import os
from dataclasses import dataclass
from urllib.parse import quote

import httpx


class DockerManagerError(RuntimeError):
    pass


@dataclass
class ContainerInfo:
    container_id: str
    status: str


@dataclass
class RsshubContainer:
    container_id: str
    name: str
    status: str
    host_port: int
    internal_url: str
    managed: bool


DOCKER_SOCKET = os.getenv("DOCKER_SOCKET", "/var/run/docker.sock")
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "tuite-tg_default")
RSSHUB_IMAGE = os.getenv("MANAGED_RSSHUB_IMAGE", "diygod/rsshub:latest")


def docker_available() -> bool:
    return os.path.exists(DOCKER_SOCKET)


def _client() -> httpx.Client:
    if not docker_available():
        raise DockerManagerError("未挂载 Docker socket，无法在网页中管理 RSSHub 容器。")
    transport = httpx.HTTPTransport(uds=DOCKER_SOCKET)
    return httpx.Client(transport=transport, base_url="http://docker", timeout=30.0)


def _request(method: str, path: str, **kwargs) -> httpx.Response:
    with _client() as client:
        resp = client.request(method, path, **kwargs)
    if resp.status_code >= 400:
        raise DockerManagerError(f"Docker API {resp.status_code}: {resp.text[:500]}")
    return resp


def ensure_image_available(image: str) -> None:
    encoded_image = quote(image, safe="")
    try:
        _request("GET", f"/images/{encoded_image}/json")
        return
    except DockerManagerError as exc:
        if "Docker API 404" not in str(exc):
            raise
    repository, tag = split_image_name(image)
    with _client() as client:
        resp = client.post(
            f"/images/create?fromImage={quote(repository, safe='')}&tag={quote(tag, safe='')}",
            timeout=300.0,
        )
    if resp.status_code >= 400:
        raise DockerManagerError(f"Docker 拉取镜像失败 {resp.status_code}: {resp.text[:500]}")


def split_image_name(image: str) -> tuple[str, str]:
    if ":" not in image.rsplit("/", 1)[-1]:
        return image, "latest"
    repository, tag = image.rsplit(":", 1)
    return repository, tag


def create_rsshub_container(
    name: str,
    host_port: int,
    twitter_auth_token: str = "",
    third_party_api: str = "",
    proxy_uri: str = "",
) -> ContainerInfo:
    ensure_image_available(RSSHUB_IMAGE)
    env = [
        "CACHE_EXPIRE=30",
        f"TWITTER_AUTH_TOKEN={twitter_auth_token}",
        f"TWITTER_THIRD_PARTY_API={third_party_api}",
        f"PROXY_URI={proxy_uri}",
    ]
    payload = {
        "Image": RSSHUB_IMAGE,
        "Env": env,
        "ExposedPorts": {"1200/tcp": {}},
        "Labels": {
            "managed-by": "tuite-tg",
            "tuite-tg-rsshub": "true",
        },
        "HostConfig": {
            "RestartPolicy": {"Name": "unless-stopped"},
            "PortBindings": {
                "1200/tcp": [
                    {
                        "HostIp": "127.0.0.1",
                        "HostPort": str(host_port),
                    }
                ]
            },
            "NetworkMode": DOCKER_NETWORK,
        },
        "NetworkingConfig": {
            "EndpointsConfig": {
                DOCKER_NETWORK: {
                    "Aliases": [name],
                }
            }
        },
    }
    resp = _request("POST", f"/containers/create?name={name}", json=payload)
    container_id = resp.json()["Id"]
    _request("POST", f"/containers/{container_id}/start")
    return ContainerInfo(container_id=container_id, status="running")


def recreate_rsshub_container(
    name: str,
    host_port: int,
    twitter_auth_token: str = "",
    third_party_api: str = "",
    proxy_uri: str = "",
    old_container_id: str = "",
) -> ContainerInfo:
    target = old_container_id or find_container_id_by_name(name)
    if target:
        remove_container(target)
    return create_rsshub_container(name, host_port, twitter_auth_token, third_party_api, proxy_uri)


def remove_container(container_id: str) -> None:
    if not container_id:
        return
    try:
        _request("POST", f"/containers/{container_id}/stop?t=10")
    except DockerManagerError:
        pass
    try:
        _request("DELETE", f"/containers/{container_id}?force=true")
    except DockerManagerError as exc:
        if "Docker API 404" in str(exc) or "No such container" in str(exc):
            return
        raise


def find_container_id_by_name(name: str) -> str:
    for container in list_rsshub_containers():
        if container.name == name:
            return container.container_id
    return ""


def inspect_container(container_id: str) -> ContainerInfo:
    resp = _request("GET", f"/containers/{container_id}/json")
    data = resp.json()
    return ContainerInfo(
        container_id=container_id,
        status=str(data.get("State", {}).get("Status", "unknown")),
    )


def container_logs(container_id: str, tail: int = 120) -> str:
    if not container_id:
        return ""
    try:
        resp = _request(
            "GET",
            f"/containers/{container_id}/logs?stdout=true&stderr=true&tail={tail}&timestamps=false",
        )
    except DockerManagerError as exc:
        return f"读取容器日志失败：{exc}"
    return decode_docker_log(resp.content)


def list_rsshub_containers() -> list[RsshubContainer]:
    resp = _request("GET", "/containers/json?all=true")
    containers = resp.json()
    results: list[RsshubContainer] = []
    for item in containers:
        labels = item.get("Labels") or {}
        names = [str(name).strip("/") for name in item.get("Names") or []]
        primary_name = names[0] if names else str(item.get("Id", ""))[:12]
        normalized_name = normalize_compose_name(primary_name)
        managed = labels.get("tuite-tg-rsshub") == "true"
        status = str(item.get("State", "unknown"))
        image = str(item.get("Image", ""))
        is_rsshub = (
            labels.get("tuite-tg-rsshub") == "true"
            or "rsshub" in primary_name.lower()
            or "rsshub" in image.lower()
        )
        if not is_rsshub:
            continue
        port = extract_host_port(item.get("Ports") or [])
        results.append(
            RsshubContainer(
                container_id=str(item.get("Id", "")),
                name=normalized_name,
                status=status,
                host_port=port,
                internal_url=f"http://{normalized_name}:1200",
                managed=managed,
            )
        )
    return sorted(results, key=lambda row: (row.host_port or 99999, row.name))


def extract_host_port(ports: list[dict]) -> int:
    for port in ports:
        if int(port.get("PrivatePort") or 0) != 1200:
            continue
        public_port = port.get("PublicPort")
        if public_port:
            return int(public_port)
    return 0


def normalize_compose_name(name: str) -> str:
    if name.startswith("tuite-tg-") and name.endswith("-1"):
        return name.removeprefix("tuite-tg-").removesuffix("-1")
    return name


def decode_docker_log(content: bytes) -> str:
    if not content:
        return ""
    output = bytearray()
    i = 0
    size = len(content)
    while i + 8 <= size:
        frame_size = int.from_bytes(content[i + 4 : i + 8], "big")
        if frame_size <= 0 or i + 8 + frame_size > size:
            break
        output.extend(content[i + 8 : i + 8 + frame_size])
        i += 8 + frame_size
    if not output:
        output.extend(content)
    return output.decode("utf-8", errors="replace")
