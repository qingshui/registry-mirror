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