import pytest
from hyrax_alerts.consumers.base_consumer import HyraxAlertsBaseConsumer


def test___iter___fails():
    """Test that __iter__ will raise a NotImplementedError
    when called on the base class.
    """
    consumer = HyraxAlertsBaseConsumer(config={})
    with pytest.raises(NotImplementedError):
        list(consumer)
