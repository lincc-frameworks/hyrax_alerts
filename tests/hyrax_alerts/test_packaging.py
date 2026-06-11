import hyrax_alerts


def test_version():
    """Check to see that we can get the package version"""
    assert hyrax_alerts.__version__ is not None
