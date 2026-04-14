"""registry-mirror 命令行入口。"""

import argparse
import atexit
import os
import shutil
import signal
import subprocess
import sys
import tempfile

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
EXIT_INTERRUPT = 130


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
        sys.exit(EXIT_DOCKER_LOAD_ERROR)


def _add_common_args(parser):
    """为子命令添加公共参数。"""
    parser.add_argument("image", help="Docker 镜像名 (如 nginx:latest, registry.example.com/myimg:v1)")
    parser.add_argument("--user", help="Registry 用户名")
    parser.add_argument("--password-stdin", action="store_true", help="从 stdin 读取密码")
    parser.add_argument("--proxy", help="HTTP/HTTPS 代理地址")
    parser.add_argument("--mirror", help="镜像源地址 (仅替代 Docker Hub)")
    parser.add_argument("--platform", default="linux/amd64", help="目标平台 (默认: linux/amd64)")
    parser.add_argument("--no-streaming", action="store_true", help="禁用流式组装（回退到先下载全部再组装的模式）")


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

    tmpdir_ref = [None]
    tar_finished = [False]

    def cleanup():
        if tmpdir_ref[0] and os.path.exists(tmpdir_ref[0]):
            shutil.rmtree(tmpdir_ref[0], ignore_errors=True)
        # 清理不完整的 tar 文件
        if not tar_finished[0] and os.path.exists(output_path):
            os.remove(output_path)

    def sigint_handler(signum, frame):
        cleanup()
        sys.exit(EXIT_INTERRUPT)

    signal.signal(signal.SIGINT, sigint_handler)
    atexit.register(cleanup)

    # 临时目录放在输出文件所在目录，避免跨分区占用额外空间
    tmpdir = tempfile.mkdtemp(prefix=".registry-mirror-tmp-", dir=output_dir)
    tmpdir_ref[0] = tmpdir

    try:
        client = RegistryClient(
            username=args.user,
            password=password,
            proxy=args.proxy,
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
            builder = StreamingImageBuilder(output_path, repo_tag, len(manifest["layers"]))

            print("下载 config...")
            config_blob_path = client.download_blob(
                registry, repository, manifest["config"]["digest"], tmpdir
            )
            builder.add_config(config_blob_path, manifest["config"]["digest"])
            os.remove(config_blob_path)

            for i, layer in enumerate(manifest["layers"], 1):
                print(f"下载 layer {i}/{len(manifest['layers'])}: {layer['digest'][:20]}...")
                layer_blob_path = client.download_blob(
                    registry, repository, layer["digest"], tmpdir
                )
                builder.add_layer(layer_blob_path, layer["digest"])
                os.remove(layer_blob_path)

            print("组装 Docker Image tar...")
            tar_digest = builder.finish()
        else:
            # 非流式模式：先下载全部再组装
            print("下载 config...")
            client.download_blob(registry, repository, manifest["config"]["digest"], tmpdir)

            for i, layer in enumerate(manifest["layers"], 1):
                print(f"下载 layer {i}/{len(manifest['layers'])}: {layer['digest'][:20]}...")
                client.download_blob(registry, repository, layer["digest"], tmpdir)

            print("组装 Docker Image tar...")
            tar_digest = build_image_tar(manifest, tmpdir, output_path, repo_tag)

        print(f"导出完成: {output_path}")
        print(f"SHA256: {tar_digest}")
        tar_finished[0] = True

        if load_after:
            docker_load(output_path)
            if cleanup_tar_on_success:
                os.remove(output_path)
                print(f"已清理临时文件: {output_path}")

    except DigestMismatchError as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(EXIT_DOWNLOAD_ERROR)
    except ValueError as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(EXIT_INPUT_ERROR)
    except Exception as e:
        if "401" in str(e) or "403" in str(e):
            print(f"认证失败: {e}", file=sys.stderr)
            print("提示: 请使用 --user 和 --password-stdin 提供认证信息", file=sys.stderr)
            sys.exit(EXIT_INPUT_ERROR)
        elif "404" in str(e):
            print(f"镜像不存在: {args.image}", file=sys.stderr)
            sys.exit(EXIT_INPUT_ERROR)
        elif isinstance(e, (ConnectionError, OSError)):
            print(f"连接失败: {e}", file=sys.stderr)
            print("提示: 请检查网络连接或使用 --proxy 设置代理", file=sys.stderr)
            sys.exit(EXIT_INPUT_ERROR)
        else:
            print(f"错误: {e}", file=sys.stderr)
            sys.exit(EXIT_DOWNLOAD_ERROR)
    finally:
        cleanup()


def cmd_save(args):
    """save 子命令：拉取镜像并保存为 tar 文件。"""
    output_path = args.output or build_default_output(args.image)
    _pull_image(args, output_path, load_after=args.load, cleanup_tar_on_success=False)


def cmd_pull(args):
    """pull 子命令：拉取镜像并直接导入 Docker。"""
    # pull 模式使用临时 tar 文件，导入成功后自动清理
    output_dir = os.getcwd()
    output_path = os.path.join(
        tempfile.mkdtemp(prefix=".registry-mirror-tmp-", dir=output_dir),
        build_default_output(args.image),
    )
    _pull_image(args, output_path, load_after=True, cleanup_tar_on_success=True)


def main():
    """命令行主入口。"""
    parser = argparse.ArgumentParser(
        prog="registry-mirror",
        description="Docker 镜像离线导出工具 — 从远端 Registry 拉取镜像并保存为本地 tar 文件",
    )
    subparsers = parser.add_subparsers(dest="command")

    # save 子命令（默认行为）
    save_parser = subparsers.add_parser("save", help="拉取镜像并保存为 tar 文件")
    _add_common_args(save_parser)
    save_parser.add_argument("-o", "--output", help="输出文件路径 (默认: <镜像名>.tar)")
    save_parser.add_argument("--load", action="store_true", help="导出后自动 docker load")
    save_parser.set_defaults(func=cmd_save)

    # pull 子命令（下载并导入 Docker）
    pull_parser = subparsers.add_parser("pull", help="拉取镜像并直接导入 Docker")
    _add_common_args(pull_parser)
    pull_parser.set_defaults(func=cmd_pull)

    # 向后兼容：无子命令时，直接传镜像名作为 save
    args = parser.parse_args()

    if args.command is None:
        # 无子命令，重新解析为 save 模式
        parser2 = argparse.ArgumentParser(
            prog="registry-mirror",
            description="Docker 镜像离线导出工具 — 从远端 Registry 拉取镜像并保存为本地 tar 文件",
        )
        parser2.add_argument("image", help="Docker 镜像名 (如 nginx:latest, registry.example.com/myimg:v1)")
        parser2.add_argument("-o", "--output", help="输出文件路径 (默认: <镜像名>.tar)")
        parser2.add_argument("--user", help="Registry 用户名")
        parser2.add_argument("--password-stdin", action="store_true", help="从 stdin 读取密码")
        parser2.add_argument("--proxy", help="HTTP/HTTPS 代理地址")
        parser2.add_argument("--mirror", help="镜像源地址 (仅替代 Docker Hub)")
        parser2.add_argument("--platform", default="linux/amd64", help="目标平台 (默认: linux/amd64)")
        parser2.add_argument("--load", action="store_true", help="导出后自动 docker load")
        parser2.add_argument("--no-streaming", action="store_true", help="禁用流式组装（回退到先下载全部再组装的模式）")
        args = parser2.parse_args()
        output_path = args.output or build_default_output(args.image)
        _pull_image(args, output_path, load_after=args.load, cleanup_tar_on_success=False)
    else:
        args.func(args)


if __name__ == "__main__":
    main()
