# registry-mirror

Docker 镜像离线导出工具 — 基于 Registry V2 API，纯 Python 实现，无需安装 Docker。

从远端 Registry 拉取镜像并保存为本地 tar 文件，可直接 `docker load -i <file>` 导入。

## 安装

```bash
pip install -e .
```

## 使用

```bash
# 基本用法
registry-mirror nginx:latest

# 指定输出路径
registry-mirror nginx:latest -o /data/nginx.tar

# 认证
registry-mirror myrepo.com/myimg:v1 --user admin --password-stdin < secret.txt

# 代理
registry-mirror nginx:latest --proxy http://127.0.0.1:7890

# 使用国内镜像源
registry-mirror nginx:latest --mirror docker.m.daocloud.io

# 指定平台
registry-mirror nginx:latest --platform linux/arm64

# 导出后自动 docker load
registry-mirror nginx:latest --load
```

## 参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `image` | Docker 镜像名 | - |
| `-o, --output` | 输出文件路径 | `<镜像名>.tar` |
| `--user` | Registry 用户名 | - |
| `--password-stdin` | 从 stdin 读取密码 | - |
| `--proxy` | HTTP/HTTPS 代理地址 | - |
| `--mirror` | 镜像源地址（仅替代 Docker Hub） | - |
| `--platform` | 目标平台 | `linux/amd64` |
| `--load` | 导出后自动 docker load | false |