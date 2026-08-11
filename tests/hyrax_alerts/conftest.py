import pytest
from hyrax_alerts.run_context import reset_run


@pytest.fixture(autouse=True)
def fresh_run_context():
    """Clear the cached run directories around every test.

    ``run_context`` caches this run's output directories at module scope, so
    without this a directory reserved by one test would leak into the next.
    """
    reset_run()
    yield
    reset_run()
