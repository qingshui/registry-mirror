import pytest
from registry_mirror.registry_client import parse_image_name


class TestParseImageName:
    def test_simple_name_with_tag(self):
        result = parse_image_name("nginx:latest")
        assert result == ("registry-1.docker.io", "library/nginx", "latest")

    def test_simple_name_without_tag(self):
        result = parse_image_name("nginx")
        assert result == ("registry-1.docker.io", "library/nginx", "latest")

    def test_name_with_registry(self):
        result = parse_image_name("registry.example.com/myimg:v1")
        assert result == ("registry.example.com", "myimg", "v1")

    def test_name_with_registry_and_path(self):
        result = parse_image_name("registry.example.com/org/myimg:v2")
        assert result == ("registry.example.com", "org/myimg", "v2")

    def test_name_with_digest(self):
        result = parse_image_name("nginx@sha256:abc123")
        assert result == ("registry-1.docker.io", "library/nginx", "sha256:abc123")

    def test_name_with_registry_and_digest(self):
        result = parse_image_name("registry.example.com/myimg@sha256:def456")
        assert result == ("registry.example.com", "myimg", "sha256:def456")

    def test_docker_hub_explicit_user(self):
        result = parse_image_name("myuser/myapp:1.0")
        assert result == ("registry-1.docker.io", "myuser/myapp", "1.0")

    def test_registry_with_port(self):
        result = parse_image_name("localhost:5000/myimg:v1")
        assert result == ("localhost:5000", "myimg", "v1")

    def test_registry_with_port_no_tag(self):
        result = parse_image_name("localhost:5000/myimg")
        assert result == ("localhost:5000", "myimg", "latest")

    def test_invalid_empty_name(self):
        with pytest.raises(ValueError):
            parse_image_name("")