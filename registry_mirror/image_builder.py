"""Docker Image tar 组装模块。"""

import hashlib
import io
import json
import os
import tarfile


def build_image_tar(manifest, blob_dir, output_path, repo_tag):
    """将下载的 blob 组装为 Docker Image tar 文件。

    Args:
        manifest: Image Manifest V2 字典
        blob_dir: blob 临时文件目录
        output_path: 输出 tar 文件路径
        repo_tag: 镜像标签（如 nginx:latest, registry.example.com/myimg:v1）

    Returns:
        输出 tar 文件的 sha256 digest
    """
    config_digest = manifest["config"]["digest"]
    layer_digests = [layer["digest"] for layer in manifest["layers"]]

    config_hash = config_digest.replace("sha256:", "")
    config_filename = f"{config_hash}.json"

    layer_entries = []
    for digest in layer_digests:
        layer_hash = digest.replace("sha256:", "")
        layer_entries.append((digest, layer_hash, f"{layer_hash}/layer.tar"))

    image_manifest = {
        "Config": config_filename,
        "RepoTags": [repo_tag],
        "Layers": [entry[2] for entry in layer_entries],
    }

    if ":" in repo_tag:
        repo_name, tag = repo_tag.rsplit(":", 1)
    else:
        repo_name = repo_tag
        tag = "latest"

    first_layer_hash = layer_digests[0].replace("sha256:", "") if layer_digests else ""

    repositories = {
        repo_name: {
            tag: first_layer_hash,
        }
    }

    with tarfile.open(output_path, "w") as tar:
        config_blob_path = os.path.join(blob_dir, config_digest.replace(":", "_"))
        tar.add(config_blob_path, arcname=config_filename)

        for digest, layer_hash, arcname in layer_entries:
            layer_blob_path = os.path.join(blob_dir, digest.replace(":", "_"))
            dir_info = tarfile.TarInfo(name=layer_hash)
            dir_info.type = tarfile.DIRTYPE
            dir_info.mode = 0o755
            tar.addfile(dir_info)
            tar.add(layer_blob_path, arcname=arcname)

        manifest_bytes = json.dumps([image_manifest]).encode()
        manifest_info = tarfile.TarInfo(name="manifest.json")
        manifest_info.size = len(manifest_bytes)
        tar.addfile(manifest_info, io.BytesIO(manifest_bytes))

        repos_bytes = json.dumps(repositories).encode()
        repos_info = tarfile.TarInfo(name="repositories")
        repos_info.size = len(repos_bytes)
        tar.addfile(repos_info, io.BytesIO(repos_bytes))

    hasher = hashlib.sha256()
    with open(output_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)

    return f"sha256:{hasher.hexdigest()}"


class StreamingImageBuilder:
    """流式 Docker Image tar 组装器，支持逐层写入以降低磁盘峰值占用。

    用法:
        builder = StreamingImageBuilder(output_path, repo_tag, layer_count)
        builder.add_config(config_blob_path, config_digest)
        for layer_blob_path, layer_digest in layers:
            builder.add_layer(layer_blob_path, layer_digest)
        tar_digest = builder.finish()

    调用顺序约束: add_config → add_layer*(可多次) → finish
    """

    _STATE_INIT = "init"
    _STATE_CONFIG_ADDED = "config_added"
    _STATE_FINISHED = "finished"

    def __init__(self, output_path, repo_tag, layer_count):
        """初始化流式组装器。

        Args:
            output_path: 输出 tar 文件路径
            repo_tag: 镜像标签（如 nginx:latest）
            layer_count: 总 layer 数量
        """
        self._output_path = output_path
        self._repo_tag = repo_tag
        self._tar = tarfile.open(output_path, "w")
        self._config_filename = None
        self._layer_entries = []
        self._state = self._STATE_INIT

    def add_config(self, config_blob_path, config_digest):
        """写入 config json 条目。

        Args:
            config_blob_path: config blob 临时文件路径
            config_digest: config 的 sha256 digest（如 sha256:abc...）

        Raises:
            RuntimeError: 如果调用顺序错误
        """
        if self._state != self._STATE_INIT:
            raise RuntimeError("add_config 必须首先调用，且只能调用一次")
        config_hash = config_digest.replace("sha256:", "")
        config_filename = f"{config_hash}.json"
        self._tar.add(config_blob_path, arcname=config_filename)
        self._config_filename = config_filename
        self._state = self._STATE_CONFIG_ADDED

    def add_layer(self, layer_blob_path, layer_digest):
        """写入一个 layer 条目（目录 + layer.tar）。

        Args:
            layer_blob_path: layer blob 临时文件路径
            layer_digest: layer 的 sha256 digest

        Raises:
            RuntimeError: 如果在 add_config 之前调用
        """
        if self._state != self._STATE_CONFIG_ADDED:
            raise RuntimeError("add_layer 必须在 add_config 之后调用")
        layer_hash = layer_digest.replace("sha256:", "")
        arcname = f"{layer_hash}/layer.tar"

        dir_info = tarfile.TarInfo(name=layer_hash)
        dir_info.type = tarfile.DIRTYPE
        dir_info.mode = 0o755
        self._tar.addfile(dir_info)

        self._tar.add(layer_blob_path, arcname=arcname)
        self._layer_entries.append((layer_digest, layer_hash, arcname))

    def finish(self):
        """写入 manifest.json 和 repositories，关闭 tar，返回 sha256 digest。

        Returns:
            输出 tar 文件的 sha256 digest

        Raises:
            RuntimeError: 如果调用顺序错误
        """
        if self._state != self._STATE_CONFIG_ADDED:
            raise RuntimeError("finish 必须在 add_config 和 add_layer 之后调用")

        first_layer_hash = self._layer_entries[0][1] if self._layer_entries else ""

        image_manifest = {
            "Config": self._config_filename,
            "RepoTags": [self._repo_tag],
            "Layers": [entry[2] for entry in self._layer_entries],
        }

        if ":" in self._repo_tag:
            repo_name, tag = self._repo_tag.rsplit(":", 1)
        else:
            repo_name = self._repo_tag
            tag = "latest"

        repositories = {
            repo_name: {
                tag: first_layer_hash,
            }
        }

        manifest_bytes = json.dumps([image_manifest]).encode()
        manifest_info = tarfile.TarInfo(name="manifest.json")
        manifest_info.size = len(manifest_bytes)
        self._tar.addfile(manifest_info, io.BytesIO(manifest_bytes))

        repos_bytes = json.dumps(repositories).encode()
        repos_info = tarfile.TarInfo(name="repositories")
        repos_info.size = len(repos_bytes)
        self._tar.addfile(repos_info, io.BytesIO(repos_bytes))

        self._tar.close()
        self._state = self._STATE_FINISHED

        hasher = hashlib.sha256()
        with open(self._output_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hasher.update(chunk)
        return f"sha256:{hasher.hexdigest()}"