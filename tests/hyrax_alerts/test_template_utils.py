import numpy as np
import pytest
from hyrax_alerts.template_utils import (
    BUNDLED_TEMPLATE_DIR,
    TRUNCATION_MARKER,
    available_bundled_templates,
    build_batch_context,
    build_environment,
    load_template,
    render,
    resolve_template_path,
)
from hyrax_alerts.writers.slack_writer import _format_batch_summary


def _write_template(directory, name, source):
    """Write a template file into `directory` and return its path."""
    path = directory / name
    path.write_text(source)
    return path


def _render_source(tmp_path, source, **context):
    """Compile a template from `source` in tmp_path and render it with `context`."""
    return render(load_template(_write_template(tmp_path, "t.jinja", source)), context)


def _records(object_ids):
    """Return a batch of records in the shape writers receive them."""
    return [{"object_id": object_id, "__hyrax_result": {"data": np.float32(0.5)}} for object_id in object_ids]


def test_py_filter_converts_numpy_array():
    """The py filter turns a numpy array into a plain Python list."""
    environment = build_environment([BUNDLED_TEMPLATE_DIR])
    template = environment.from_string("{{ value | py }}")

    assert template.render(value=np.array([[1, 2], [3, 4]])) == "[[1, 2], [3, 4]]"


def test_py_filter_converts_numpy_scalar():
    """The py filter turns a numpy scalar into a Python number."""
    environment = build_environment([BUNDLED_TEMPLATE_DIR])
    template = environment.from_string("{{ (value | py) is float }}")

    assert template.render(value=np.float64(0.25)) == "True"


def test_py_filter_passes_plain_values_through():
    """A value with no tolist() is returned unchanged."""
    environment = build_environment([BUNDLED_TEMPLATE_DIR])
    template = environment.from_string("{{ value | py }}")

    assert template.render(value="already a string") == "already a string"


def test_missing_nested_field_renders_empty(tmp_path):
    """Chaining through a field a record lacks renders empty rather than raising."""
    context = {"records": [{"object_id": "a"}]}

    rendered = _render_source(tmp_path, "[{{ records[0].data.missing.field }}]", **context)

    assert rendered == "[]"


def test_resolve_template_path_finds_file_on_disk(tmp_path):
    """A path to an existing file resolves to that file."""
    path = _write_template(tmp_path, "mine.jinja", "hello")

    assert resolve_template_path(str(path)) == path.resolve()


def test_resolve_template_path_finds_bundled_template_by_name():
    """A bare name resolves against the bundled template directory."""
    assert resolve_template_path("default.jinja") == BUNDLED_TEMPLATE_DIR / "default.jinja"


def test_resolve_template_path_reports_both_locations_tried():
    """A miss raises ValueError naming both candidates and the bundled options."""
    with pytest.raises(ValueError, match="nope.jinja") as error:
        resolve_template_path("nope.jinja")

    message = str(error.value)
    assert str(BUNDLED_TEMPLATE_DIR) in message
    assert "default.jinja" in message


def test_load_template_raises_on_syntax_error(tmp_path):
    """A syntax error becomes a ValueError carrying the file and line number."""
    path = _write_template(tmp_path, "broken.jinja", "fine\n{% for x in %}\n")

    with pytest.raises(ValueError, match="broken.jinja"):
        load_template(path)


def test_user_template_can_include_a_bundled_template(tmp_path):
    """A template on disk resolves {% include %} against the bundled directory."""
    path = _write_template(tmp_path, "wraps.jinja", 'header\n{% include "default.jinja" %}')
    batch = _records([101])

    rendered = render(load_template(path), build_batch_context(batch, max_object_ids=10))

    assert rendered.startswith("header\n")
    assert _format_batch_summary(batch, max_object_ids=10) in rendered


def test_user_template_can_include_a_sibling_file(tmp_path):
    """A template's own directory is on the search path, so siblings resolve."""
    _write_template(tmp_path, "part.jinja", "shared bit")
    path = _write_template(tmp_path, "whole.jinja", 'start {% include "part.jinja" %}')

    assert render(load_template(path), {}) == "start shared bit"


def test_render_truncates_long_output(tmp_path):
    """Output longer than max_length is truncated with a marker."""
    template = load_template(_write_template(tmp_path, "long.jinja", "{{ 'x' * 500 }}"))

    rendered = render(template, {}, max_length=100)

    assert len(rendered) == 100
    assert rendered.endswith(TRUNCATION_MARKER)


def test_render_leaves_short_output_alone(tmp_path):
    """Output within max_length is returned unchanged."""
    template = load_template(_write_template(tmp_path, "short.jinja", "hello"))

    assert render(template, {}, max_length=100) == "hello"


def test_build_batch_context_exposes_records_count_and_ids():
    """The context carries the batch, its size, and the object ids."""
    batch = _records(["a", "b"])

    context = build_batch_context(batch, max_object_ids=5)

    assert context["records"] is batch
    assert context["count"] == 2
    assert context["object_ids"] == ["a", "b"]
    assert context["max_object_ids"] == 5


def test_build_batch_context_skips_records_without_object_id():
    """count reports every record, but object_ids only covers records that have one."""
    context = build_batch_context([{"object_id": "a"}, {"__hyrax_result": {"data": 1}}])

    assert context["count"] == 2
    assert context["object_ids"] == ["a"]


@pytest.mark.parametrize("name", available_bundled_templates())
def test_bundled_template_renders(name):
    """Every bundled template compiles and renders against a sample batch."""
    template = load_template(resolve_template_path(name))

    rendered = render(template, build_batch_context(_records([101, 102]), max_object_ids=10))

    assert "101" in rendered


@pytest.mark.parametrize("object_ids", [[], [101], [101, 102, 103], list(range(15))])
def test_default_template_matches_builtin_summary(object_ids):
    """default.jinja reproduces _format_batch_summary, so the two cannot drift."""
    batch = _records(object_ids)
    template = load_template(resolve_template_path("default.jinja"))

    rendered = render(template, build_batch_context(batch, max_object_ids=10))

    assert rendered == _format_batch_summary(batch, max_object_ids=10)
