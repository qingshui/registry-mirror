"""端到端集成测试 — 使用 mock Registry 验证完整流程。"""

import hashlib
import json
import os
import tarfile
import tempfile

import pytest

from registry_mirror.registry_client import RegistryClient, parse_image_name
from registry_mirror.image_builder import build_image_tar


def _create_mock_manifest():
    """创建一个模拟的 manifest 和对应的 blob 数据。"""
    config_data = json.dumps({
        "architecture": "amd64",
        "os": "linux",
        "config": {},
        "rootfs": {"type": "layers", "diff_ids": []},
    }).encode()
    config_digest = f"sha256:{hashlib.sha256(config_data).hexdigest()}"

    layer_data = b"fake layer tar content"
    layer_digest = f"sha256:{hashlib.sha256(layer_data).hexdigest()}"

    manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "config": {
            "mediaType": "application/vnd.docker.container.image.v1+json",
            "size": len(config_data),
            "digest": config_digest,
        },
        "layers": [
            {
                "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                "size": len(layer_data),
                "digest": layer_digest,
            }
        ],
    }
    return manifest, config_data, config_digest, layer_data, layer_digest


class TestEndToEnd:
    def test_full_pull_and_build(self, requests_mock):
        """测试完整流程: 解析 → manifest → 下载 → 组装。"""
        manifest, config_data, config_digest, layer_data, layer_digest = _create_mock_manifest()

        requests_mock.get(
            "https://registry-1.docker.io/v2/library/nginx/manifests/latest",
            json=manifest,
            headers={"Content-Type": "application/vnd.docker.distribution.manifest.v2+json"},
        )
        requests_mock.get(
            f"https://registry-1.docker.io/v2/library/nginx/blobs/{config_digest}",
            content=config_data,
        )
        requests_mock.get(
            f"https://registry-1.docker.io/v2/library/nginx/blobs/{layer_digest}",
            content=layer_data,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "nginx.tar")

            client = RegistryClient()
            registry, repository, reference = parse_image_name("nginx:latest")
            fetched_manifest = client.fetch_manifest(registry, repository, reference)

            client.download_blob(registry, repository, fetched_manifest["config"]["digest"], tmpdir)
            for layer in fetched_manifest["layers"]:
                client.download_blob(registry, repository, layer["digest"], tmpdir)

            tar_digest = build_image_tar(fetched_manifest, tmpdir, output_path, "nginx:latest")

            assert os.path.exists(output_path)
            with tarfile.open(output_path) as tf:
                names = tf.getnames()
                assert "manifest.json" in names
                assert "repositories" in names

                manifest_json = json.load(tf.extractfile("manifest.json"))
                assert manifest_json[0]["RepoTags"] == ["nginx:latest"]