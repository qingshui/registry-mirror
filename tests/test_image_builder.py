import hashlib
import json
import tarfile
import tempfile
import os

import pytest
from registry_mirror.image_builder import build_image_tar


class TestBuildImageTar:
    def _make_blob(self, content):
        if isinstance(content, str):
            content = content.encode()
        digest = f"sha256:{hashlib.sha256(content).hexdigest()}"
        return content, digest

    def _setup_blobs(self, tmpdir, config_content, config_digest, layer_content, layer_digest):
        config_path = os.path.join(tmpdir, config_digest.replace(":", "_"))
        with open(config_path, "wb") as f:
            f.write(config_content if isinstance(config_content, bytes) else config_content.encode())
        layer_path = os.path.join(tmpdir, layer_digest.replace(":", "_"))
        with open(layer_path, "wb") as f:
            f.write(layer_content)

    def test_build_tar_structure(self):
        config_content, config_digest = self._make_blob('{"architecture":"amd64"}')
        layer_content, layer_digest = self._make_blob(b"fake layer data")
        manifest = {
            "config": {"digest": config_digest, "size": len(config_content)},
            "layers": [{"digest": layer_digest, "size": len(layer_content)}],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            self._setup_blobs(tmpdir, config_content, config_digest, layer_content, layer_digest)
            output_path = os.path.join(tmpdir, "output.tar")
            result = build_image_tar(manifest=manifest, blob_dir=tmpdir, output_path=output_path, repo_tag="nginx:latest")
            assert result.startswith("sha256:")
            with tarfile.open(output_path) as tf:
                names = tf.getnames()
                config_filename = config_digest.replace("sha256:", "") + ".json"
                layer_dir = layer_digest.replace("sha256:", "")
                assert config_filename in names
                assert f"{layer_dir}/layer.tar" in names
                assert "manifest.json" in names
                assert "repositories" in names

    def test_manifest_json_content(self):
        config_content, config_digest = self._make_blob('{"architecture":"amd64"}')
        layer_content, layer_digest = self._make_blob(b"fake layer data")
        manifest = {
            "config": {"digest": config_digest, "size": len(config_content)},
            "layers": [{"digest": layer_digest, "size": len(layer_content)}],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            self._setup_blobs(tmpdir, config_content, config_digest, layer_content, layer_digest)
            output_path = os.path.join(tmpdir, "output.tar")
            build_image_tar(manifest=manifest, blob_dir=tmpdir, output_path=output_path, repo_tag="nginx:latest")
            with tarfile.open(output_path) as tf:
                manifest_json = json.load(tf.extractfile("manifest.json"))
                assert len(manifest_json) == 1
                assert manifest_json[0]["RepoTags"] == ["nginx:latest"]
                assert manifest_json[0]["Config"].endswith(".json")

    def test_repositories_simple_image(self):
        config_content, config_digest = self._make_blob('{"architecture":"amd64"}')
        layer_content, layer_digest = self._make_blob(b"fake layer data")
        manifest = {
            "config": {"digest": config_digest, "size": len(config_content)},
            "layers": [{"digest": layer_digest, "size": len(layer_content)}],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            self._setup_blobs(tmpdir, config_content, config_digest, layer_content, layer_digest)
            output_path = os.path.join(tmpdir, "output.tar")
            build_image_tar(manifest=manifest, blob_dir=tmpdir, output_path=output_path, repo_tag="nginx:latest")
            with tarfile.open(output_path) as tf:
                repos = json.load(tf.extractfile("repositories"))
                assert "nginx" in repos
                assert "latest" in repos["nginx"]

    def test_repositories_registry_image(self):
        config_content, config_digest = self._make_blob('{"architecture":"amd64"}')
        layer_content, layer_digest = self._make_blob(b"fake layer data")
        manifest = {
            "config": {"digest": config_digest, "size": len(config_content)},
            "layers": [{"digest": layer_digest, "size": len(layer_content)}],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            self._setup_blobs(tmpdir, config_content, config_digest, layer_content, layer_digest)
            output_path = os.path.join(tmpdir, "output.tar")
            build_image_tar(manifest=manifest, blob_dir=tmpdir, output_path=output_path, repo_tag="registry.example.com/myimg:v1")
            with tarfile.open(output_path) as tf:
                repos = json.load(tf.extractfile("repositories"))
                assert "registry.example.com/myimg" in repos
                assert "v1" in repos["registry.example.com/myimg"]

    def test_return_sha256_matches_file(self):
        config_content, config_digest = self._make_blob('{"architecture":"amd64"}')
        layer_content, layer_digest = self._make_blob(b"fake layer data")
        manifest = {
            "config": {"digest": config_digest, "size": len(config_content)},
            "layers": [{"digest": layer_digest, "size": len(layer_content)}],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            self._setup_blobs(tmpdir, config_content, config_digest, layer_content, layer_digest)
            output_path = os.path.join(tmpdir, "output.tar")
            result_digest = build_image_tar(manifest=manifest, blob_dir=tmpdir, output_path=output_path, repo_tag="nginx:latest")
            hasher = hashlib.sha256()
            with open(output_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hasher.update(chunk)
            expected = f"sha256:{hasher.hexdigest()}"
            assert result_digest == expected