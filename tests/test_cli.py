import os
import subprocess
import pytest
from unittest.mock import patch, MagicMock

from registry_mirror.cli import (
    sanitize_filename,
    build_default_output,
    check_disk_space,
    docker_load,
    EXIT_DOCKER_NOT_FOUND,
    EXIT_DOCKER_LOAD_ERROR,
)


class TestSanitizeFilename:
    def test_simple_tag(self):
        assert sanitize_filename("nginx:latest") == "nginx_latest"

    def test_with_registry(self):
        assert sanitize_filename("registry.example.com/myimg:v1") == "registry.example.com_myimg_v1"

    def test_with_digest(self):
        assert sanitize_filename("nginx@sha256:abc123") == "nginx_sha256abc123"


class TestBuildDefaultOutput:
    def test_simple(self):
        assert build_default_output("nginx:latest") == "nginx_latest.tar"

    def test_with_registry(self):
        assert build_default_output("registry.example.com/myimg:v1") == "registry.example.com_myimg_v1.tar"


class TestCheckDiskSpace:
    def test_sufficient_space_streaming(self):
        manifest = {
            "config": {"size": 100},
            "layers": [{"size": 200}],
        }
        import tempfile
        check_disk_space(manifest, tempfile.gettempdir(), streaming=True)

    def test_sufficient_space_non_streaming(self):
        manifest = {
            "config": {"size": 100},
            "layers": [{"size": 200}],
        }
        import tempfile
        check_disk_space(manifest, tempfile.gettempdir(), streaming=False)

    def test_insufficient_space(self):
        manifest = {
            "config": {"size": 10 ** 15},
            "layers": [{"size": 10 ** 15}],
        }
        import tempfile
        with pytest.raises(SystemExit) as exc_info:
            check_disk_space(manifest, tempfile.gettempdir(), streaming=True)
        assert exc_info.value.code == 3


class TestDockerLoadExitCodes:
    def test_docker_not_found_exits_5(self):
        """docker 未安装时应使用退出码 5。"""
        with patch("registry_mirror.cli.subprocess.run", side_effect=FileNotFoundError):
            with pytest.raises(SystemExit) as exc_info:
                docker_load("/tmp/test.tar")
            assert exc_info.value.code == EXIT_DOCKER_NOT_FOUND

    def test_docker_load_failure_exits_4(self):
        """docker load 失败时应使用退出码 4。"""
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "load error"
        with patch("registry_mirror.cli.subprocess.run", return_value=mock_result):
            with pytest.raises(SystemExit) as exc_info:
                docker_load("/tmp/test.tar")
            assert exc_info.value.code == EXIT_DOCKER_LOAD_ERROR
