"""端到端集成测试 — 使用 mock Registry 验证完整流程。"""

import hashlib
import json
import os
import tarfile
import tempfile

import pytest

from registry_mirror.registry_client import RegistryClient, parse_image_name
from registry_mirror.image_builder import build_image_tar, StreamingImageBuilder


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

    def test_multi_layer_with_auth(self, requests_mock):
        """测试多 layer + Bearer 认证流程。"""
        config_data = json.dumps({"architecture": "amd64", "os": "linux"}).encode()
        config_digest = f"sha256:{hashlib.sha256(config_data).hexdigest()}"
        layer1_data = b"layer 1 content"
        layer1_digest = f"sha256:{hashlib.sha256(layer1_data).hexdigest()}"
        layer2_data = b"layer 2 content"
        layer2_digest = f"sha256:{hashlib.sha256(layer2_data).hexdigest()}"

        manifest = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {
                "mediaType": "application/vnd.docker.container.image.v1+json",
                "size": len(config_data),
                "digest": config_digest,
            },
            "layers": [
                {"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip", "size": len(layer1_data), "digest": layer1_digest},
                {"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip", "size": len(layer2_data), "digest": layer2_digest},
            ],
        }

        # 第一次请求返回 401，触发认证
        requests_mock.get(
            "https://registry.example.com/v2/myimg/manifests/v1",
            [
                {"status_code": 401, "headers": {"WWW-Authenticate": 'Bearer realm="https://auth.example.com/token",service="registry.example.com",scope="repository:myimg:pull"'}},
                {"json": manifest, "headers": {"Content-Type": "application/vnd.docker.distribution.manifest.v2+json"}},
            ],
        )
        requests_mock.get(
            "https://auth.example.com/token",
            json={"token": "test-token"},
        )
        requests_mock.get(
            f"https://registry.example.com/v2/myimg/blobs/{config_digest}",
            content=config_data,
        )
        requests_mock.get(
            f"https://registry.example.com/v2/myimg/blobs/{layer1_digest}",
            content=layer1_data,
        )
        requests_mock.get(
            f"https://registry.example.com/v2/myimg/blobs/{layer2_digest}",
            content=layer2_data,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "myimg.tar")

            client = RegistryClient()
            registry, repository, reference = parse_image_name("registry.example.com/myimg:v1")
            fetched_manifest = client.fetch_manifest(registry, repository, reference)

            # 使用流式组装
            with StreamingImageBuilder(output_path, "registry.example.com/myimg:v1", 2) as builder:
                config_blob = client.download_blob(registry, repository, fetched_manifest["config"]["digest"], tmpdir)
                builder.add_config(config_blob, fetched_manifest["config"]["digest"])
                os.remove(config_blob)

                for layer in fetched_manifest["layers"]:
                    layer_blob = client.download_blob(registry, repository, layer["digest"], tmpdir)
                    builder.add_layer(layer_blob, layer["digest"])
                    os.remove(layer_blob)

                tar_digest = builder.finish()

            assert os.path.exists(output_path)
            with tarfile.open(output_path) as tf:
                manifest_json = json.load(tf.extractfile("manifest.json"))
                assert len(manifest_json[0]["Layers"]) == 2

    def test_multi_arch_selection(self, requests_mock):
        """测试多架构 manifest list 选择正确平台。"""
        manifest_list = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": [
                {"mediaType": "application/vnd.docker.distribution.manifest.v2+json", "digest": "sha256:amd64digest", "platform": {"architecture": "amd64", "os": "linux"}},
                {"mediaType": "application/vnd.docker.distribution.manifest.v2+json", "digest": "sha256:arm64digest", "platform": {"architecture": "arm64", "os": "linux"}},
            ],
        }

        config_data = json.dumps({"architecture": "arm64", "os": "linux"}).encode()
        config_digest = f"sha256:{hashlib.sha256(config_data).hexdigest()}"
        layer_data = b"arm64 layer"
        layer_digest = f"sha256:{hashlib.sha256(layer_data).hexdigest()}"

        arm64_manifest = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {"mediaType": "application/vnd.docker.container.image.v1+json", "size": len(config_data), "digest": config_digest},
            "layers": [{"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip", "size": len(layer_data), "digest": layer_digest}],
        }

        requests_mock.get(
            "https://registry-1.docker.io/v2/library/nginx/manifests/latest",
            json=manifest_list,
            headers={"Content-Type": "application/vnd.docker.distribution.manifest.list.v2+json"},
        )
        requests_mock.get(
            "https://registry-1.docker.io/v2/library/nginx/manifests/sha256:arm64digest",
            json=arm64_manifest,
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

        client = RegistryClient()
        manifest = client.fetch_manifest(
            "registry-1.docker.io", "library/nginx", "latest",
            platform="linux/arm64",
        )
        assert manifest["config"]["digest"] == config_digest