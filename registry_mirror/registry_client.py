"""Docker Registry V2 API 客户端"""

import hashlib
import os
import re
import time

import requests


DOCKER_HUB_REGISTRY = "registry-1.docker.io"


class DigestMismatchError(Exception):
    """Blob digest 校验失败。"""
    pass


def parse_image_name(image: str) -> tuple:
    """解析 Docker 镜像名为 (registry_host, repository, tag_or_digest)。

    Args:
        image: Docker 镜像名，如 nginx:latest, registry.example.com/myimg:v1, nginx@sha256:abc

    Returns:
        (registry_host, repository, tag_or_digest) 元组

    Raises:
        ValueError: 镜像名格式无效
    """
    if not image or not image.strip():
        raise ValueError("镜像名不能为空")

    image = image.strip()

    # 分离 digest（@sha256:...）
    digest = None
    if "@" in image:
        image, digest = image.rsplit("@", 1)
        if not digest.startswith("sha256:"):
            raise ValueError(f"不支持的 digest 格式: {digest}")

    # 分离 tag
    # tag 的冒号必须在最后一个 / 之后（否则是端口号）
    tag = "latest"
    last_slash = image.rfind("/")
    if ":" in image:
        last_colon = image.rfind(":")
        # 冒号在最后一个 / 之后才是 tag
        if last_colon > last_slash:
            after_colon = image[last_colon + 1:]
            if not after_colon.isdigit():
                tag = after_colon
                image = image[:last_colon]

    # 判断 registry
    parts = image.split("/")
    if len(parts) == 1:
        registry = DOCKER_HUB_REGISTRY
        repository = f"library/{parts[0]}"
    elif len(parts) == 2:
        first_part = parts[0]
        if "." in first_part or ":" in first_part:
            registry = first_part
            repository = parts[1]
        else:
            registry = DOCKER_HUB_REGISTRY
            repository = image
    else:
        registry = parts[0]
        if "." not in registry and ":" not in registry:
            registry = DOCKER_HUB_REGISTRY
            repository = image
        else:
            repository = "/".join(parts[1:])

    reference = digest if digest else tag
    return (registry, repository, reference)


def parse_www_authenticate(header: str) -> tuple:
    """解析 WWW-Authenticate 头。"""
    match = re.match(r"(Bearer|Basic)\s+(.+)", header, re.IGNORECASE)
    if not match:
        return (header.lower(), {})

    auth_type = match.group(1).lower()
    params_str = match.group(2)

    params = {}
    for m in re.finditer(r'(\w+)="([^"]*)"', params_str):
        params[m.group(1)] = m.group(2)

    return (auth_type, params)


class RegistryClient:
    """Docker Registry V2 API 客户端。"""

    def __init__(self, username=None, password=None, proxy=None):
        self.session = requests.Session()
        self.username = username
        self.password = password
        self._token_cache = {}

        if proxy:
            self.session.proxies = {
                "http": proxy,
                "https": proxy,
            }

    def _get_bearer_token(self, realm, service, scope):
        """获取 Bearer Token。"""
        params = {
            "service": service,
            "scope": scope,
        }
        if self.username and self.password:
            resp = self.session.get(realm, params=params, auth=(self.username, self.password))
        else:
            resp = self.session.get(realm, params=params)
        resp.raise_for_status()
        data = resp.json()
        return data.get("token") or data.get("access_token")

    def _auth_for_scope(self, registry, repository, www_authenticate, force_refresh=False):
        """根据 WWW-Authenticate 头进行认证。"""
        auth_type, params = parse_www_authenticate(www_authenticate)

        if auth_type == "bearer":
            scope = params.get("scope", f"repository:{repository}:pull")
            cache_key = (registry, repository, scope)

            if not force_refresh and cache_key in self._token_cache:
                return self._token_cache[cache_key]

            token = self._get_bearer_token(
                realm=params["realm"],
                service=params.get("service", ""),
                scope=scope,
            )
            self._token_cache[cache_key] = token
            return token
        elif auth_type == "basic":
            if not self.username or not self.password:
                raise ValueError("此 Registry 需要 Basic 认证，请提供 --user 和 --password-stdin")
            self.session.auth = (self.username, self.password)
            return None
        else:
            raise ValueError(f"不支持的认证方式: {auth_type}")