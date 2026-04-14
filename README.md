# registry-mirror

Docker 镜像离线导出工具 — 基于 Registry V2 API，纯 Python 实现，无需安装 Docker。

从远端 Registry 拉取镜像并保存为本地 tar 文件，可直接 `docker load -i <file>` 导入。

## 安装

```bash
pip install -e .
```

## 使用

### save — 导出为 tar 文件

```bash
# 基本用法（默认导出到当前目录）
registry-mirror save nginx:latest

# 向后兼容：省略 save 也行
registry-mirror nginx:latest

# 指定输出路径
registry-mirror save nginx:latest -o /data/nginx.tar

# 导出后自动 docker load
registry-mirror save nginx:latest --load
```

### pull — 下载并直接导入 Docker

```bash
# 下载镜像并导入 Docker，自动清理临时 tar 文件
registry-mirror pull nginx:latest
```

### 通用参数

```bash
# 认证
registry-mirror pull myrepo.com/myimg:v1 --user admin --password-stdin < secret.txt

# 代理
registry-mirror pull nginx:latest --proxy http://127.0.0.1:7890

# 使用国内镜像源
registry-mirror pull nginx:latest --mirror docker.m.daocloud.io

# 指定平台
registry-mirror pull nginx:latest --platform linux/arm64
```

## 子命令

| 子命令 | 说明 |
|--------|------|
| `save` | 拉取镜像并保存为 tar 文件 |
| `pull` | 拉取镜像并直接导入 Docker（自动清理临时文件） |

## 参数

| 参数 | 适用子命令 | 说明 | 默认值 |
|------|-----------|------|--------|
| `image` | save/pull | Docker 镜像名 | - |
| `-o, --output` | save | 输出文件路径 | `<镜像名>.tar` |
| `--user` | save/pull | Registry 用户名 | - |
| `--password-stdin` | save/pull | 从 stdin 读取密码 | - |
| `--proxy` | save/pull | HTTP/HTTPS 代理地址 | - |
| `--mirror` | save/pull | 镜像源地址（仅替代 Docker Hub） | - |
| `--platform` | save/pull | 目标平台 | `linux/amd64` |
| `--load` | save | 导出后自动 docker load | false |
| `--no-streaming` | save/pull | 禁用流式组装 | false |
