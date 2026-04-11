import os
import pytest
from unittest.mock import patch

from registry_mirror.cli import sanitize_filename, build_default_output, check_disk_space


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
    def test_sufficient_space(self):
        manifest = {
            "config": {"size": 100},
            "layers": [{"size": 200}],
        }
        import tempfile
        check_disk_space(manifest, tempfile.gettempdir())

    def test_insufficient_space(self):
        manifest = {
            "config": {"size": 10 ** 15},
            "layers": [{"size": 10 ** 15}],
        }
        import tempfile
        with pytest.raises(SystemExit) as exc_info:
            check_disk_space(manifest, tempfile.gettempdir())
        assert exc_info.value.code == 3
