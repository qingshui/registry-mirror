import hashlib
import tempfile

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


class TestRegistryAuth:
    def test_parse_www_authenticate_bearer(self):
        from registry_mirror.registry_client import parse_www_authenticate
        header = 'Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/nginx:pull"'
        result = parse_www_authenticate(header)
        assert result == ("bearer", {
            "realm": "https://auth.docker.io/token",
            "service": "registry.docker.io",
            "scope": "repository:library/nginx:pull",
        })

    def test_parse_www_authenticate_basic(self):
        from registry_mirror.registry_client import parse_www_authenticate
        header = 'Basic realm="Registry Realm"'
        result = parse_www_authenticate(header)
        assert result == ("basic", {"realm": "Registry Realm"})

    def test_get_token_with_bearer(self, requests_mock):
        from registry_mirror.registry_client import RegistryClient
        requests_mock.get(
            "https://auth.docker.io/token",
            json={"token": "test-token-123", "access_token": "test-token-123"},
        )
        client = RegistryClient()
        token = client._get_bearer_token(
            realm="https://auth.docker.io/token",
            service="registry.docker.io",
            scope="repository:library/nginx:pull",
        )
        assert token == "test-token-123"

    def test_token_expired_refresh(self, requests_mock):
        from registry_mirror.registry_client import RegistryClient
        requests_mock.get(
            "https://auth.docker.io/token",
            json={"token": "new-token-456"},
        )
        client = RegistryClient()
        client._token_cache[("registry-1.docker.io", "library/nginx", "repository:library/nginx:pull")] = "old-expired-token"
        token = client._auth_for_scope(
            "registry-1.docker.io",
            "library/nginx",
            'Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/nginx:pull"',
            force_refresh=True,
        )
        assert token == "new-token-456"


class TestFetchManifest:
    def test_fetch_manifest_v2(self, requests_mock):
        from registry_mirror.registry_client import RegistryClient
        manifest = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {"mediaType": "application/vnd.docker.container.image.v1+json", "size": 100, "digest": "sha256:aaa"},
            "layers": [{"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip", "size": 200, "digest": "sha256:bbb"}],
        }
        requests_mock.get(
            "https://registry-1.docker.io/v2/library/nginx/manifests/latest",
            json=manifest,
            headers={"Content-Type": "application/vnd.docker.distribution.manifest.v2+json"},
        )
        client = RegistryClient()
        result = client.fetch_manifest("registry-1.docker.io", "library/nginx", "latest")
        assert result["config"]["digest"] == "sha256:aaa"
        assert len(result["layers"]) == 1

    def test_fetch_manifest_with_auth_retry(self, requests_mock):
        from registry_mirror.registry_client import RegistryClient
        manifest = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {"mediaType": "application/vnd.docker.container.image.v1+json", "size": 100, "digest": "sha256:aaa"},
            "layers": [],
        }
        requests_mock.get(
            "https://registry.example.com/v2/myimg/manifests/v1",
            [
                {"status_code": 401, "headers": {"WWW-Authenticate": 'Bearer realm="https://auth.example.com/token",service="registry.example.com",scope="repository:myimg:pull"'}},
                {"json": manifest, "headers": {"Content-Type": "application/vnd.docker.distribution.manifest.v2+json"}},
            ],
        )
        requests_mock.get(
            "https://auth.example.com/token",
            json={"token": "test-bearer-token"},
        )
        client = RegistryClient()
        result = client.fetch_manifest("registry.example.com", "myimg", "v1")
        assert result["config"]["digest"] == "sha256:aaa"

    def test_fetch_manifest_list_resolves_platform(self, requests_mock):
        from registry_mirror.registry_client import RegistryClient
        manifest_list = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": [
                {"mediaType": "application/vnd.docker.distribution.manifest.v2+json", "digest": "sha256:amd64digest", "platform": {"architecture": "amd64", "os": "linux"}},
                {"mediaType": "application/vnd.docker.distribution.manifest.v2+json", "digest": "sha256:arm64digest", "platform": {"architecture": "arm64", "os": "linux"}},
            ],
        }
        manifest_v2 = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {"mediaType": "application/vnd.docker.container.image.v1+json", "size": 100, "digest": "sha256:aaa"},
            "layers": [{"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip", "size": 200, "digest": "sha256:bbb"}],
        }
        requests_mock.get(
            "https://registry-1.docker.io/v2/library/nginx/manifests/latest",
            json=manifest_list,
            headers={"Content-Type": "application/vnd.docker.distribution.manifest.list.v2+json"},
        )
        requests_mock.get(
            "https://registry-1.docker.io/v2/library/nginx/manifests/sha256:amd64digest",
            json=manifest_v2,
            headers={"Content-Type": "application/vnd.docker.distribution.manifest.v2+json"},
        )
        client = RegistryClient()
        result = client.fetch_manifest("registry-1.docker.io", "library/nginx", "latest", platform="linux/amd64")
        assert result["config"]["digest"] == "sha256:aaa"

    def test_fetch_manifest_list_no_matching_platform(self, requests_mock):
        from registry_mirror.registry_client import RegistryClient
        manifest_list = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.docker.distribution.manifest.list.v2+json",
            "manifests": [
                {"mediaType": "application/vnd.docker.distribution.manifest.v2+json", "digest": "sha256:amd64digest", "platform": {"architecture": "amd64", "os": "linux"}},
            ],
        }
        requests_mock.get(
            "https://registry-1.docker.io/v2/library/nginx/manifests/latest",
            json=manifest_list,
            headers={"Content-Type": "application/vnd.docker.distribution.manifest.list.v2+json"},
        )
        client = RegistryClient()
        with pytest.raises(ValueError, match="平台"):
            client.fetch_manifest("registry-1.docker.io", "library/nginx", "latest", platform="linux/arm64")


class TestDownloadBlob:
    def test_download_blob_to_file(self, requests_mock):
        from registry_mirror.registry_client import RegistryClient
        content = b"test layer content"
        digest = "sha256:" + hashlib.sha256(content).hexdigest()
        requests_mock.get(
            f"https://registry-1.docker.io/v2/library/nginx/blobs/{digest}",
            content=content,
        )
        client = RegistryClient()
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = client.download_blob("registry-1.docker.io", "library/nginx", digest, tmpdir)
            with open(filepath, "rb") as f:
                assert f.read() == content

    def test_download_blob_digest_mismatch(self, requests_mock):
        from registry_mirror.registry_client import RegistryClient, DigestMismatchError
        content = b"test layer content"
        wrong_digest = "sha256:" + "0" * 64
        requests_mock.get(
            f"https://registry-1.docker.io/v2/library/nginx/blobs/{wrong_digest}",
            content=content,
        )
        client = RegistryClient()
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(DigestMismatchError):
                client.download_blob("registry-1.docker.io", "library/nginx", wrong_digest, tmpdir)