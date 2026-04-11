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