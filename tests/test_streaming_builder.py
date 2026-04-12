import hashlib
import json
import os
import tarfile
import tempfile

import pytest
from registry_mirror.image_builder import StreamingImageBuilder, build_image_tar


class TestStreamingImageBuilder:
    def _make_blob(self, content):
        if isinstance(content, str):
            content = content.encode()
        digest = f"sha256:{hashlib.sha256(content).hexdigest()}"
        return content, digest

    def _write_blob(self, tmpdir, content, digest):
        path = os.path.join(tmpdir, digest.replace(":", "_"))
        with open(path, "wb") as f:
            f.write(content if isinstance(content, bytes) else content.encode())
        return path

    def test_basic_flow(self):
        """测试基本 add_config → add_layer → finish 流程。"""
        config_content, config_digest = self._make_blob('{"architecture":"amd64"}')
        layer_content, layer_digest = self._make_blob(b"fake layer data")

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = self._write_blob(tmpdir, config_content, config_digest)
            layer_path = self._write_blob(tmpdir, layer_content, layer_digest)
            output_path = os.path.join(tmpdir, "output.tar")

            builder = StreamingImageBuilder(output_path, "nginx:latest", 1)
            builder.add_config(config_path, config_digest)
            builder.add_layer(layer_path, layer_digest)
            tar_digest = builder.finish()

            assert tar_digest.startswith("sha256:")
            with tarfile.open(output_path) as tf:
                names = tf.getnames()
                config_filename = config_digest.replace("sha256:", "") + ".json"
                layer_dir = layer_digest.replace("sha256:", "")
                assert config_filename in names
                assert f"{layer_dir}/layer.tar" in names
                assert "manifest.json" in names
                assert "repositories" in names

    def test_state_protection_add_config_twice(self):
        """add_config 只能调用一次。"""
        config_content, config_digest = self._make_blob('{"architecture":"amd64"}')
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = self._write_blob(tmpdir, config_content, config_digest)
            output_path = os.path.join(tmpdir, "output.tar")
            builder = StreamingImageBuilder(output_path, "nginx:latest", 1)
            builder.add_config(config_path, config_digest)
            with pytest.raises(RuntimeError, match="add_config"):
                builder.add_config(config_path, config_digest)

    def test_state_protection_layer_before_config(self):
        """add_layer 必须在 add_config 之后调用。"""
        layer_content, layer_digest = self._make_blob(b"fake layer data")
        with tempfile.TemporaryDirectory() as tmpdir:
            layer_path = self._write_blob(tmpdir, layer_content, layer_digest)
            output_path = os.path.join(tmpdir, "output.tar")
            builder = StreamingImageBuilder(output_path, "nginx:latest", 1)
            with pytest.raises(RuntimeError, match="add_config"):
                builder.add_layer(layer_path, layer_digest)

    def test_state_protection_finish_without_config(self):
        """finish 必须在 add_config 之后调用。"""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "output.tar")
            builder = StreamingImageBuilder(output_path, "nginx:latest", 1)
            with pytest.raises(RuntimeError, match="add_config"):
                builder.finish()

    def test_multi_layer(self):
        """测试多 layer 场景。"""
        config_content, config_digest = self._make_blob('{"architecture":"amd64"}')
        layer1_content, layer1_digest = self._make_blob(b"layer 1 data")
        layer2_content, layer2_digest = self._make_blob(b"layer 2 data")
        layer3_content, layer3_digest = self._make_blob(b"layer 3 data")

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = self._write_blob(tmpdir, config_content, config_digest)
            layer1_path = self._write_blob(tmpdir, layer1_content, layer1_digest)
            layer2_path = self._write_blob(tmpdir, layer2_content, layer2_digest)
            layer3_path = self._write_blob(tmpdir, layer3_content, layer3_digest)
            output_path = os.path.join(tmpdir, "output.tar")

            builder = StreamingImageBuilder(output_path, "myimg:v2", 3)
            builder.add_config(config_path, config_digest)
            builder.add_layer(layer1_path, layer1_digest)
            builder.add_layer(layer2_path, layer2_digest)
            builder.add_layer(layer3_path, layer3_digest)
            tar_digest = builder.finish()

            with tarfile.open(output_path) as tf:
                manifest_json = json.load(tf.extractfile("manifest.json"))
                assert manifest_json[0]["RepoTags"] == ["myimg:v2"]
                assert len(manifest_json[0]["Layers"]) == 3

                repos = json.load(tf.extractfile("repositories"))
                assert "myimg" in repos
                assert "v2" in repos["myimg"]

    def test_sha256_matches_file(self):
        """finish() 返回的 sha256 与文件实际内容一致。"""
        config_content, config_digest = self._make_blob('{"architecture":"amd64"}')
        layer_content, layer_digest = self._make_blob(b"fake layer data")

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = self._write_blob(tmpdir, config_content, config_digest)
            layer_path = self._write_blob(tmpdir, layer_content, layer_digest)
            output_path = os.path.join(tmpdir, "output.tar")

            builder = StreamingImageBuilder(output_path, "nginx:latest", 1)
            builder.add_config(config_path, config_digest)
            builder.add_layer(layer_path, layer_digest)
            result_digest = builder.finish()

            hasher = hashlib.sha256()
            with open(output_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hasher.update(chunk)
            expected = f"sha256:{hasher.hexdigest()}"
            assert result_digest == expected

    def test_consistency_with_build_image_tar(self):
        """流式组装与 build_image_tar 产出一致的 tar 结构。"""
        config_content, config_digest = self._make_blob('{"architecture":"amd64"}')
        layer_content, layer_digest = self._make_blob(b"fake layer data")
        manifest = {
            "config": {"digest": config_digest, "size": len(config_content)},
            "layers": [{"digest": layer_digest, "size": len(layer_content)}],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            # 非流式组装
            non_streaming_dir = os.path.join(tmpdir, "non_streaming")
            os.makedirs(non_streaming_dir)
            config_path1 = self._write_blob(non_streaming_dir, config_content, config_digest)
            layer_path1 = self._write_blob(non_streaming_dir, layer_content, layer_digest)
            output1 = os.path.join(tmpdir, "non_streaming.tar")
            build_image_tar(manifest, non_streaming_dir, output1, "nginx:latest")

            # 流式组装
            streaming_dir = os.path.join(tmpdir, "streaming")
            os.makedirs(streaming_dir)
            config_path2 = self._write_blob(streaming_dir, config_content, config_digest)
            layer_path2 = self._write_blob(streaming_dir, layer_content, layer_digest)
            output2 = os.path.join(tmpdir, "streaming.tar")

            builder = StreamingImageBuilder(output2, "nginx:latest", 1)
            builder.add_config(config_path2, config_digest)
            builder.add_layer(layer_path2, layer_digest)
            builder.finish()

            # 对比 tar 内的文件名集合
            with tarfile.open(output1) as tf1, tarfile.open(output2) as tf2:
                names1 = set(tf1.getnames())
                names2 = set(tf2.getnames())
                assert names1 == names2

                # 对比 manifest.json
                m1 = json.load(tf1.extractfile("manifest.json"))
                m2 = json.load(tf2.extractfile("manifest.json"))
                assert m1 == m2

                # 对比 repositories
                r1 = json.load(tf1.extractfile("repositories"))
                r2 = json.load(tf2.extractfile("repositories"))
                assert r1 == r2
