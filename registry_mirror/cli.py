"""registry-mirror 命令行入口。"""

import argparse
import atexit
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from registry_mirror.registry_client import (
    DigestMismatchError,
    RegistryClient,
    parse_image_name,
)
from registry_mirror.image_builder import StreamingImageBuilder, build_image_tar


EXIT_SUCCESS = 0
EXIT_INPUT_ERROR = 1
EXIT_DOWNLOAD_ERROR = 2
EXIT_DISK_ERROR = 3
EXIT_DOCKER_LOAD_ERROR = 4
EXIT_DOCKER_NOT_FOUND = 5
EXIT_INTERRUPT = 130

_DOWNLOAD_WORKERS = 4


def sanitize_filename(image_name):
    """将镜像名转为文件系统安全的文件名。"""
    if "@" in image_name:
        # digest 引用：@ 替换为 _，: 删除（sha256:xxx -> sha256xxx）
        return image_name.replace("/", "_").replace("@", "_").replace(":", "")
    else:
        # tag 引用：/ 和 : 替换为 _
        return image_name.replace("/", "_").replace(":", "_")


def build_default_output(image_name):
    """根据镜像名生成默认输出文件名。"""
    return sanitize_filename(image_name) + ".tar"


def check_disk_space(manifest, output_dir, streaming=True):
    """检查磁盘空间是否足够。

    Args:
        manifest: Image Manifest V2 字典
        output_dir: 输出目录
        streaming: 是否使用流式组装模式
    """
    config_size = manifest.get("config", {}).get("size", 0)
    layer_sizes = [layer.get("size", 0) for layer in manifest.get("layers", [])]
    total_blob_size = config_size + sum(layer_sizes)

    if streaming:
        # 流式模式：峰值 = tar 大小 + 当前处理的单个 blob
        max_single_layer = max(layer_sizes) if layer_sizes else 0
        estimated_tar_size = total_blob_size * 1.05
        estimated_peak = estimated_tar_size + max_single_layer
    else:
        # 非流式模式：峰值 = 全部 blob + tar 文件
        estimated_peak = total_blob_size * 2.1

    # 额外预留 5% 用于文件系统开销
    required = int(estimated_peak * 1.05)

    disk_usage = shutil.disk_usage(output_dir)
    if disk_usage.free < required:
        print(
            f"错误: 磁盘空间不足。需要约 {required / 1024 / 1024:.1f} MB，"
            f"可用 {disk_usage.free / 1024 / 1024:.1f} MB",
            file=sys.stderr,
        )
        sys.exit(EXIT_DISK_ERROR)


def _format_progress(downloaded, total, start_time):
    """格式化下载进度字符串，如 '12.3/45.6 MB (27%) 2.1 MB/s'。"""
    elapsed = max(time.time() - start_time, 0.001)
    speed = downloaded / elapsed

    def fmt_bytes(b):
        if b >= 1 << 30:
            return f"{b / (1 << 30):.1f} GB"
        elif b >= 1 << 20:
            return f"{b / (1 << 20):.1f} MB"
        else:
            return f"{b / (1 << 10):.1f} KB"

    if total and total > 0:
        pct = downloaded * 100 // total
        return f"{fmt_bytes(downloaded)}/{fmt_bytes(total)} ({pct}%) {fmt_bytes(speed)}/s"
    else:
        return f"{fmt_bytes(downloaded)} {fmt_bytes(speed)}/s"


def _make_progress_callback(start_time):
    """创建下载进度回调函数。"""
    def callback(downloaded, total):
        msg = f"\r  {_format_progress(downloaded, total, start_time)}"
        sys.stderr.write(msg)
        sys.stderr.flush()
    return callback


def docker_load(tar_path):
    """执行 docker load 导入镜像。

    Returns:
        True 如果导入成功，False 如果 docker 未安装

    Raises:
        SystemExit: 如果 docker load 执行失败
    """
    try:
        result = subprocess.run(
            ["docker", "load", "-i", tar_path],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"docker load 失败: {result.stderr}", file=sys.stderr)
            sys.exit(EXIT_DOCKER_LOAD_ERROR)
        print(result.stdout)
        return True
    except FileNotFoundError:
        print(
            "警告: docker 未安装，无法执行 docker load。"
            f"tar 文件已保存到 {tar_path}，可手动执行 docker load -i {tar_path}",
            file=sys.stderr,
        )
        sys.exit(EXIT_DOCKER_NOT_FOUND)


def _create_common_parser():
    """创建公共参数的父解析器，供子命令和向后兼容模式共享。"""
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("image", help="Docker 镜像名 (如 nginx:latest, registry.example.com/myimg:v1)")
    parent.add_argument("--user", help="Registry 用户名")
    parent.add_argument("--password-stdin", action="store_true", help="从 stdin 读取密码")
    parent.add_argument("--proxy", help="HTTP/HTTPS 代理地址")
    parent.add_argument("--mirror", help="镜像源地址 (仅替代 Docker Hub)")
    parent.add_argument("--platform", default="linux/amd64", help="目标平台 (默认: linux/amd64)")
    parent.add_argument("--no-streaming", action="store_true", help="禁用流式组装（回退到先下载全部再组装的模式）")
    parent.add_argument("--insecure", action="store_true", help="使用 HTTP 而非 HTTPS 连接 Registry (用于私有仓库)")
    return parent


def _pull_image(args, output_path, load_after=False, cleanup_tar_on_success=False):
    """拉取镜像并导出为 tar 文件的核心逻辑。

    Args:
        args: 解析后的命令行参数
        output_path: 输出 tar 文件路径
        load_after: 是否在导出后执行 docker load
        cleanup_tar_on_success: 是否在 docker load 成功后删除 tar 文件
    """
    password = None
    if args.password_stdin:
        password = sys.stdin.read().strip()

    try:
        registry, repository, reference = parse_image_name(args.image)
    except ValueError as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(EXIT_INPUT_ERROR)

    if args.mirror:
        if registry == "registry-1.docker.io":
            registry = args.mirror
            print(f"使用镜像源: {registry}")
        else:
            print(f"警告: --mirror 仅对 Docker Hub 镜像生效，已忽略", file=sys.stderr)

    output_dir = os.path.dirname(os.path.abspath(output_path))

    tmpdir_path = None
    tar_finished = False
    original_sigint = signal.getsignal(signal.SIGINT)

    def cleanup():
        nonlocal tmpdir_path, tar_finished
        if tmpdir_path and os.path.exists(tmpdir_path):
            shutil.rmtree(tmpdir_path, ignore_errors=True)
        # 清理不完整的 tar 文件
        if not tar_finished and os.path.exists(output_path):
            os.remove(output_path)

    def sigint_handler(signum, frame):
        cleanup()
        signal.signal(signal.SIGINT, original_sigint)
        sys.exit(EXIT_INTERRUPT)

    signal.signal(signal.SIGINT, sigint_handler)
    atexit.register(cleanup)

    # 临时目录放在输出文件所在目录，避免跨分区占用额外空间
    tmpdir_path = tempfile.mkdtemp(prefix=".registry-mirror-tmp-", dir=output_dir)

    try:
        client = RegistryClient(
            username=args.user,
            password=password,
            proxy=args.proxy,
            insecure=getattr(args, 'insecure', False),
        )

        print(f"拉取 manifest: {args.image}")
        manifest = client.fetch_manifest(registry, repository, reference, platform=args.platform)
        print(f"Config: {manifest['config']['digest']}")
        print(f"Layers: {len(manifest['layers'])} 个")

        streaming = not args.no_streaming
        check_disk_space(manifest, output_dir, streaming=streaming)

        repo_tag = args.image

        if streaming:
            # 流式组装：逐层下载 → 写入 tar → 删除 blob，降低峰值磁盘占用
            with StreamingImageBuilder(output_path, repo_tag, len(manifest["layers"])) as builder:

                print("下载 config...")
                start_time = time.time()
                config_blob_path = client.download_blob(
                    registry, repository, manifest["config"]["digest"], tmpdir_path,
                    progress_callback=_make_progress_callback(start_time),
                )
                sys.stderr.write("\n")
                builder.add_config(config_blob_path, manifest["config"]["digest"])
                os.remove(config_blob_path)

                # 并行下载 layer，按序消费
                layer_digests = [layer["digest"] for layer in manifest["layers"]]
                layer_count = len(layer_digests)

                def _download_layer(idx, digest):
                    """下载单个 layer，返回 (索引, 文件路径)。"""
                    print(f"下载 layer {idx + 1}/{layer_count}: {digest[:20]}...")
                    st = time.time()
                    path = client.download_blob(
                        registry, repository, digest, tmpdir_path,
                        progress_callback=_make_progress_callback(st),
                    )
                    sys.stderr.write("\n")
                    return idx, path

                with ThreadPoolExecutor(max_workers=_DOWNLOAD_WORKERS) as executor:
                    # 提交所有下载任务
                    futures = [
                        executor.submit(_download_layer, i, d)
                        for i, d in enumerate(layer_digests)
                    ]
                    # 按序消费结果
                    for i, future in enumerate(futures):
                        idx, path = future.result()
                        builder.add_layer(path, layer_digests[idx])
                        os.remove(path)

                print("组装 Docker Image tar...")
                tar_digest = builder.finish()
        else:
            # 非流式模式：先并行下载全部再组装
            print("下载 config...")
            start_time = time.time()
            client.download_blob(
                registry, repository, manifest["config"]["digest"], tmpdir,
                progress_callback=_make_progress_callback(start_time),
            )
            sys.stderr.write("\n")

            layer_count = len(manifest["layers"])

            def _download_layer_non_streaming(idx, digest):
                """下载单个 layer。"""
                print(f"下载 layer {idx + 1}/{layer_count}: {digest[:20]}...")
                st = time.time()
                client.download_blob(
                    registry, repository, digest, tmpdir,
                    progress_callback=_make_progress_callback(st),
                )
                sys.stderr.write("\n")

            with ThreadPoolExecutor(max_workers=_DOWNLOAD_WORKERS) as executor:
                futures = [
                    executor.submit(
                        _download_layer_non_streaming, i, layer["digest"]
                    )
                    for i, layer in enumerate(manifest["layers"])
                ]
                for future in as_completed(futures):
                    future.result()  # 传播异常

            print("组装 Docker Image tar...")
            tar_digest = build_image_tar(manifest, tmpdir_path, output_path, repo_tag)

        print(f"导出完成: {output_path}")
        print(f"SHA256: {tar_digest}")
        tar_finished = True

        if load_after:
            docker_load(output_path)
            if cleanup_tar_on_success:
                os.remove(output_path)
                print(f"已清理临时文件: {output_path}")

    except DigestMismatchError as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(EXIT_DOWNLOAD_ERROR)
    except requests.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else 0
        if status_code in (401, 403):
            print(f"认证失败: {e}", file=sys.stderr)
            print("提示: 请使用 --user 和 --password-stdin 提供认证信息", file=sys.stderr)
            sys.exit(EXIT_INPUT_ERROR)
        elif status_code == 404:
            print(f"镜像不存在: {args.image}", file=sys.stderr)
            sys.exit(EXIT_INPUT_ERROR)
        else:
            print(f"HTTP 错误 {status_code}: {e}", file=sys.stderr)
            sys.exit(EXIT_DOWNLOAD_ERROR)
    except ValueError as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(EXIT_INPUT_ERROR)
    except (ConnectionError, OSError) as e:
        print(f"连接失败: {e}", file=sys.stderr)
        print("提示: 请检查网络连接或使用 --proxy 设置代理", file=sys.stderr)
        sys.exit(EXIT_INPUT_ERROR)
    except Exception as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(EXIT_DOWNLOAD_ERROR)
    finally:
        cleanup()
        signal.signal(signal.SIGINT, original_sigint)


def cmd_save(args):
    """save 子命令：拉取镜像并保存为 tar 文件。"""
    output_path = args.output or build_default_output(args.image)
    _pull_image(args, output_path, load_after=args.load, cleanup_tar_on_success=False)


def cmd_pull(args):
    """pull 子命令：拉取镜像并直接导入 Docker。"""
    # pull 模式：tar 输出到当前目录，导入成功后自动清理
    output_path = os.path.join(os.getcwd(), build_default_output(args.image))
    _pull_image(args, output_path, load_after=True, cleanup_tar_on_success=True)


def main():
    """命令行主入口。"""
    common_parser = _create_common_parser()

    parser = argparse.ArgumentParser(
        prog="registry-mirror",
        description="Docker 镜像离线导出工具 — 从远端 Registry 拉取镜像并保存为本地 tar 文件",
    )
    subparsers = parser.add_subparsers(dest="command")

    # save 子命令（默认行为）
    save_parser = subparsers.add_parser("save", parents=[common_parser], help="拉取镜像并保存为 tar 文件")
    save_parser.add_argument("-o", "--output", help="输出文件路径 (默认: <镜像名>.tar)")
    save_parser.add_argument("--load", action="store_true", help="导出后自动 docker load")
    save_parser.set_defaults(func=cmd_save)

    # pull 子命令（下载并导入 Docker）
    pull_parser = subparsers.add_parser("pull", parents=[common_parser], help="拉取镜像并直接导入 Docker")
    pull_parser.set_defaults(func=cmd_pull)

    # 向后兼容：无子命令时，直接传镜像名作为 save
    args = parser.parse_args()

    if args.command is None:
        # 无子命令，重新解析为 save 模式
        compat_parser = argparse.ArgumentParser(
            prog="registry-mirror",
            parents=[common_parser],
            description="Docker 镜像离线导出工具 — 从远端 Registry 拉取镜像并保存为本地 tar 文件",
        )
        compat_parser.add_argument("-o", "--output", help="输出文件路径 (默认: <镜像名>.tar)")
        compat_parser.add_argument("--load", action="store_true", help="导出后自动 docker load")
        args = compat_parser.parse_args()
        output_path = args.output or build_default_output(args.image)
        _pull_image(args, output_path, load_after=args.load, cleanup_tar_on_success=False)
    else:
        args.func(args)


if __name__ == "__main__":
    main()
