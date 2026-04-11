"""测试 fixture 配置。"""
import pytest
import requests_mock as rm


@pytest.fixture
def requests_mock():
    """提供 requests_mock fixture。"""
    with rm.Mocker() as m:
        yield m