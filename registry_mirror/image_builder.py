"""Docker Image tar 组装模块。"""

import hashlib
import io
import json
import os
import tarfile


SHA256_PREFIX = "sha256:"
_READ_CHUNK_SIZE = 1 << 20  # 1 MB


def strip_sha256_prefix(digest: str) -> str:
    """移除 digest 的 'sha256:' 前缀。"""
    return digest.replace(SHA256_PREFIX, "")


def parse_repo_tag(repo_tag: str) -> tuple:
    """解析 'name:tag' 为 (repo_name, tag)，tag 默认 'latest'。"""
    if ":" in repo_tag:
        repo_name, tag = repo_tag.rsplit(":", 1)
    else:
        repo_name = repo_tag
        tag = "latest"
    return repo_name, tag


def build_manifest_json(config_filename, repo_tag, layer_arcnames):
    """构建 Docker Image tar 的 manifest.json 条目。"""
    return {
        "Config": config_filename,
        "RepoTags": [repo_tag],
        "Layers": layer_arcnames,
    }


def build_repositories_json(repo_tag, first_layer_hash):
    """构建 Docker Image tar 的 repositories 字典。"""
    repo_name, tag = parse_repo_tag(repo_tag)
    return {repo_name: {tag: first_layer_hash}}


def digest_to_blob_filename(digest: str) -> str:
    """将 digest (如 sha256:abc) 转为文件系统安全的文件名 (如 sha256_abc)。"""
    return digest.replace(":", "_")


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

    config_hash = strip_sha256_prefix(config_digest)
    config_filename = f"{config_hash}.json"

    layer_entries = []
    for digest in layer_digests:
        layer_hash = strip_sha256_prefix(digest)
        layer_entries.append((digest, layer_hash, f"{layer_hash}/layer.tar"))

    image_manifest = build_manifest_json(
        config_filename, repo_tag, [entry[2] for entry in layer_entries]
    )

    first_layer_hash = strip_sha256_prefix(layer_digests[0]) if layer_digests else ""
    repositories = build_repositories_json(repo_tag, first_layer_hash)

    with tarfile.open(output_path, "w") as tar:
        config_blob_path = os.path.join(blob_dir, digest_to_blob_filename(config_digest))
        tar.add(config_blob_path, arcname=config_filename)

        for digest, layer_hash, arcname in layer_entries:
            layer_blob_path = os.path.join(blob_dir, digest_to_blob_filename(digest))
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
        for chunk in iter(lambda: f.read(_READ_CHUNK_SIZE), b""):
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._tar is not None and self._state != self._STATE_FINISHED:
            self._tar.close()
            # 清理不完整的输出文件
            if os.path.exists(self._output_path):
                os.remove(self._output_path)
        return False

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
        config_hash = strip_sha256_prefix(config_digest)
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
        layer_hash = strip_sha256_prefix(layer_digest)
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

        image_manifest = build_manifest_json(
            self._config_filename, self._repo_tag,
            [entry[2] for entry in self._layer_entries],
        )
        repositories = build_repositories_json(self._repo_tag, first_layer_hash)

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
            for chunk in iter(lambda: f.read(_READ_CHUNK_SIZE), b""):
                hasher.update(chunk)
        return f"sha256:{hasher.hexdigest()}"