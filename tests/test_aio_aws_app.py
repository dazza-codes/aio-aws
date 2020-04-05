
# from starlette.responses import HTMLResponse
from starlette.testclient import TestClient
import pytest

from aio_aws.aio_aws_app import app


@pytest.fixture
def client():
    return TestClient(app)


def test_home(client):
    response = client.get('/')
    assert response.status_code == 200
    assert 'Hello, world!' in response.content.decode('utf-8')


def test_missing(client):
    response = client.get('/fake-path')
    assert response.status_code == 404
    assert 'Page not found.' in response.content.decode('utf-8')
