import pytest
from app import app as flask_app

@pytest.fixture
def app():
    yield flask_app
@pytest.fixture
def client(app):
    return app.test_client()

def test_jobs(app, client):
    res = client.get('/jobs')
    assert res.status_code == 200

def test_jobs_id(app, client):
    res = client.get('/jobs/1')
    assert res.status_code == 200

def test_test(app, client):
    res = client.get('/test')
    assert res.status_code == 200