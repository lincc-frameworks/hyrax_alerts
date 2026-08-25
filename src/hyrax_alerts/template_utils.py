"""Jinja rendering helpers for writers that emit human-readable messages.

Notification writers (Slack today; email, Discord, or a generic webhook later)
all face the same problem: the message body is a matter of taste, and baking one
format into the package means every wording change is a code change. This module
holds the shared machinery for letting users supply their own Jinja template
file instead.

The functions here are deliberately free of logging and of any fallback policy.
They either succeed or raise, and the calling writer decides what a failure means
for it. See :class:`hyrax_alerts.writers.slack_writer.HyraxAlertsSlackWriter` for
the reference usage.
"""

from pathlib import Path

from jinja2 import ChainableUndefined, Environment, FileSystemLoader, Template
from jinja2.exceptions import TemplateNotFound, TemplateSyntaxError

# Templates bundled with the package. Users can name one of these directly
# (``message_template = "detailed.jinja"``) or copy one as a starting point.
BUNDLED_TEMPLATE_DIR = Path(__file__).parent / "message_templates"

# Slack's chat.postMessage caps the `text` field at 40,000 characters. A template
# that loops over every record without slicing will sail past that on a large
# batch, and the resulting API error is far less obvious than a truncated message.
SLACK_TEXT_LIMIT = 40_000

TRUNCATION_MARKER = "\n… (truncated)"


def _to_python(value):
    """Convert a numpy value to a plain Python object, for the ``py`` filter.

    Model output reaches templates as numpy types. Two cases want converting:

    - A multi-dimensional array, which numpy prints across several lines
      (``[[1 2]\\n [3 4]]``); ``tolist()`` keeps it on one line.
    - Any array being fed to Jinja's list filters (``join``, ``sum``, ``first``),
      which expect plain Python sequences.

    Scalars are usually better left alone: numpy prints ``np.float32(0.97)`` as
    ``0.97``, while ``tolist()`` widens it to the float64 expansion
    ``0.9700000286102295``. Values without a ``tolist`` are returned unchanged.
    """
    tolist = getattr(value, "tolist", None)
    return tolist() if callable(tolist) else value


def build_environment(search_path: list[Path | str]) -> Environment:
    """Build the Jinja environment used to render writer messages.

    Parameters
    ----------
    search_path : list[Path | str]
        Directories to search for templates, in order. Callers should include
        the directory holding the user's template followed by
        ``BUNDLED_TEMPLATE_DIR``, so that ``{% extends "default.jinja" %}``
        resolves against the templates shipped with the package.

    Returns
    -------
    Environment
        A configured environment with the ``py`` filter registered.
    """
    environment = Environment(
        loader=FileSystemLoader(search_path),
        # Messages are Slack mrkdwn, not HTML. Autoescaping would turn a literal
        # `&`, `<`, or `>` into an HTML entity in the posted message.
        autoescape=False,
        # Record contents are consumer- and model-dependent: the keys under
        # `data` come from the configured fields, and the keys under
        # `__hyrax_result` come from whatever the model returned. A template
        # referencing a field some batch happens to lack should render empty
        # rather than abort the message.
        undefined=ChainableUndefined,
        # Without these, a `{% for %}` tag sitting on its own indented line
        # emits that line's newline and leading whitespace into the message.
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=False,
    )
    environment.filters["py"] = _to_python
    return environment


def available_bundled_templates() -> list[str]:
    """Return the sorted names of the template files shipped with the package."""
    if not BUNDLED_TEMPLATE_DIR.is_dir():
        return []
    return sorted(path.name for path in BUNDLED_TEMPLATE_DIR.glob("*.jinja"))


def resolve_template_path(value: str | Path) -> Path:
    """Resolve a configured template value to a file on disk.

    Two forms are accepted, tried in this order:

    1. A filesystem path - absolute, relative to the current directory, or
       starting with ``~``.
    2. A bare filename, which is looked up in the bundled template directory,
       so ``message_template = "detailed.jinja"`` works with no path at all.

    Parameters
    ----------
    value : str | Path
        The configured ``message_template`` value.

    Returns
    -------
    Path
        The resolved template file.

    Raises
    ------
    ValueError
        If neither form locates an existing file.
    """
    candidate = Path(value).expanduser()
    if candidate.is_file():
        return candidate.resolve()

    bundled = BUNDLED_TEMPLATE_DIR / str(value)
    if bundled.is_file():
        return bundled

    # Name both places we looked, so the user can tell a typo in their own path
    # from a wrong guess at a bundled name.
    raise ValueError(
        f"Could not find message template '{value}'. Looked for the file "
        f"'{candidate}' and for a bundled template at '{bundled}'. "
        f"Bundled templates: {', '.join(available_bundled_templates()) or 'none'}."
    )


def load_template(path: Path) -> Template:
    """Compile the template at ``path``.

    The template is loaded through a loader searching its own directory and the
    bundled template directory, so a user template can ``{% include %}`` a
    sibling file or ``{% extends "default.jinja" %}``.

    Parameters
    ----------
    path : Path
        A template file, as returned by :func:`resolve_template_path`.

    Returns
    -------
    Template
        The compiled template.

    Raises
    ------
    ValueError
        If the template contains a syntax error or cannot be read. Callers are
        expected to compile at startup so that a broken template fails before
        any alerts are processed.
    """
    environment = build_environment([path.parent, BUNDLED_TEMPLATE_DIR])
    try:
        return environment.get_template(path.name)
    except TemplateSyntaxError as error:
        location = error.filename or str(path)
        raise ValueError(
            f"Invalid message template '{location}', line {error.lineno}: {error.message}"
        ) from error
    except TemplateNotFound as error:
        raise ValueError(f"Could not read message template '{path}': {error}") from error


def build_batch_context(result_batch: list[dict], **extra) -> dict:
    """Build the variables made available to a message template.

    Records are passed through unchanged rather than reshaped into a
    template-friendly view, so that template authors and ``post_process`` /
    ``post_filter`` authors work against exactly one record schema.

    Note that model output lives under the ``__hyrax_result`` key, which reads
    most clearly with subscript syntax in a template:
    ``{{ record['__hyrax_result']['data'] }}``.

    Parameters
    ----------
    result_batch : list[dict]
        The post-processed, post-filtered records for one batch.
    **extra
        Additional variables to expose, for example ``max_object_ids``.

    Returns
    -------
    dict
        A mapping with ``records``, ``count``, and ``object_ids``, plus anything
        passed via ``extra``.
    """
    return {
        "records": result_batch,
        "count": len(result_batch),
        "object_ids": [record["object_id"] for record in result_batch if "object_id" in record],
        **extra,
    }


def render(template: Template, context: dict, max_length: int = SLACK_TEXT_LIMIT) -> str:
    """Render a template and clamp the result to ``max_length`` characters.

    Parameters
    ----------
    template : Template
        A compiled template, as returned by :func:`load_template`.
    context : dict
        The template variables, typically from :func:`build_batch_context`.
    max_length : int, optional
        Maximum length of the returned string, by default ``SLACK_TEXT_LIMIT``.

    Returns
    -------
    str
        The rendered message, stripped of surrounding whitespace and truncated
        with a trailing marker if it exceeded ``max_length``.

    Raises
    ------
    Exception
        Anything the render raises is propagated, and callers decide whether a
        failure should fall back to a built-in message or abort. Note that Jinja
        raises ``TemplateError`` only for its own failures - an exception from an
        expression inside the template keeps its original type - so a caller
        wanting to survive any bad template must catch broadly.
    """
    message = template.render(**context).strip()
    if len(message) <= max_length:
        return message
    return message[: max_length - len(TRUNCATION_MARKER)] + TRUNCATION_MARKER
