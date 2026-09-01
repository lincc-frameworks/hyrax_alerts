# Customizable Slack messages via Jinja template files

## Context

`HyraxAlertsSlackWriter` currently posts one hard-coded message shape per batch, built by
`_format_batch_summary` in `src/hyrax_alerts/writers/slack_writer.py`: a count, a noun, and up to
`max_object_ids` object ids. Everything else a user might want to see — a model score from
`__hyrax_result`, a field from the consumer's `data` payload, different wording, an `@here` on a
high-confidence hit — requires editing the package.

The only customization surface today is `post_process` / `post_filter`, dotted paths to Python
callables loaded by `load_callable` (`src/hyrax_alerts/callable_loader.py`) and bound with
`MethodType` in `src/hyrax_alerts/writers/base_writer.py`. Those change *which records* reach the
writer, not *how they read*. Making a user write and install a Python module just to reword a
notification is too high a bar.

Jinja closes that gap: loops and conditionals cover the real cases (per-record lines,
threshold-gated mentions), and the customization stays in a file the user owns and can edit
without touching the package.

**Outcome:** an optional `message_template` config key holding a **path to a `.jinja` file**, plus a
bundled `message_templates/` directory of working examples to copy from. Absent the key, behavior is
byte-for-byte what it is today.

## Decisions

- **Template files on disk, not inline TOML strings.** Ship example `.jinja` files; the user writes
  their own and points at it by path. Templates get real files, real line numbers in error
  messages, syntax highlighting, and diffs.
- **Plain-text templates only.** Renders the `text=` argument. The key namespace stays open for a
  `blocks_template` (Block Kit JSON) later without redesign.
- **No `message_formatter` dotted-path escape hatch.** One customization mechanism. Users needing
  more subclass `HyraxAlertsSlackWriter` — `__init_subclass__` auto-registers it, so
  `writer_class = "MySlackWriter"` works once imported.
- **Shared `template_utils` module**, not writer-local, so a future email/Discord/webhook writer
  reuses it.
- **Split failure handling.** Syntax errors and missing files raise at `__init__`; render errors log
  and fall back to the built-in summary.

## Design

### New directory: `src/hyrax_alerts/message_templates/`

Two files to start:

- **`default.jinja`** — reproduces today's `_format_batch_summary` output. The copy-me starting
  point.
- **`detailed.jinja`** — one bulleted line per record showing `object_id` and the model result,
  with truncation and an empty-batch guard. Demonstrates the whole context surface.

These are *not* loaded when `message_template` is unset. `_format_batch_summary` remains the
zero-config default and the render-failure fallback, so back-compat is exact and the fallback path
touches no filesystem. A test asserts `default.jinja` and `_format_batch_summary` agree, so the two
can't drift.

**Packaging:** verified by building a wheel — `default_config.toml` ships today with no
`[tool.setuptools.package-data]` section, because setuptools_scm's file finder plus setuptools'
`include-package-data` default picks up git-tracked files. The `.jinja` files ship the same way,
**provided they are `git add`-ed.** An untracked template file silently vanishes from the wheel; if
that proves fragile, add an explicit `package-data` entry.

### Template resolution

`message_template` is a path, resolved in two steps so both "my own file" and "the bundled example"
work:

1. Treat the value as a filesystem path (absolute, or relative to cwd, `~` expanded). If it is an
   existing file, use it.
2. Otherwise treat it as a bare name under `message_templates/`, so
   `message_template = "detailed.jinja"` works with no path at all.

Failing both, raise `ValueError` naming *both* locations tried and listing the available bundled
template names — a missing-file error should tell the user what their options are.

Templates are loaded through a `FileSystemLoader` over `[<resolved file's directory>,
<bundled message_templates dir>]` rather than `from_string(path.read_text())`. That costs two lines
and buys `{% include %}` / `{% extends "default.jinja" %}`, so a user can start from the bundled
default and override one block.

The template is compiled once in `__init__` and held. Jinja's `auto_reload` therefore never fires —
editing a template mid-run has no effect until restart. That's the deliberate consequence of
fail-fast: re-fetching per batch would move syntax errors back to runtime.

### The template context

| Name | Value |
|---|---|
| `records` | the post-processed, post-filtered `list[dict]` — exactly what `write()` receives |
| `count` | `len(records)` |
| `object_ids` | `[r["object_id"] for r in records if "object_id" in r]` |
| `max_object_ids` | the config value, so templates can honor the same truncation budget |

Records are passed **as-is** rather than reshaped into a template-friendly view. One schema, and it
is the same one `post_process` / `post_filter` authors already work against.

Two consequences, documented in the class docstring and in `detailed.jinja`'s comments:

- `__hyrax_result` needs subscript syntax for clarity: `{{ r['__hyrax_result']['data'] }}`.
  (Dotted access happens to work — Jinja's `getattr` falls back to `getitem` on dicts — but
  subscript reads better and won't surprise anyone.)
- Values are frequently numpy scalars/arrays. A `py` filter converts them via `.tolist()`.

  **Correction found during implementation:** the original rationale for `py` — that numpy renders
  ugly and `.tolist()` renders cleanly — is backwards for scalars under numpy 2. `str(np.float32(0.97))`
  is `0.97`, while `.tolist()` widens it to the float64 expansion `0.9700000286102295`. So `py` is
  documented and used narrowly: it is for arrays, where it keeps a multi-dimensional value on one
  line (numpy prints `[[1 2]\n [3 4]]` across two) and makes Jinja's list filters work. Scalars
  should be rendered bare, and `detailed.jinja` does so.

Environment settings and why:

- `autoescape=False` — Slack mrkdwn is not HTML; escaping would mangle `&`, `<`, `>`.
- `undefined=ChainableUndefined` — `data` keys are consumer-dependent and `__hyrax_result` keys are
  model-dependent, so `{{ r.data.missing.field }}` must render empty, not explode. This matches the
  existing defensive `if "object_id" in record` posture.
- `trim_blocks=True, lstrip_blocks=True` — in a standalone `.jinja` file, authors naturally put
  `{% for %}` / `{% endfor %}` on their own indented lines, and without these two flags every such
  tag emits a stray newline plus its leading whitespace into the Slack message.
- `keep_trailing_newline=False` — template files end with a newline that Slack doesn't need.
- **Plain `Environment`, not `SandboxedEnvironment`.** The template is named by the same config file
  that can already specify `post_process = "any.module.any_function"`. A sandbox here would be
  security theater at a strictly lower trust boundary than one that already exists.

### New file: `src/hyrax_alerts/template_utils.py`

Pure and logger-free so it stays trivially testable; the writer owns the fallback policy.

- `BUNDLED_TEMPLATE_DIR = Path(__file__).parent / "message_templates"`
- `SLACK_TEXT_LIMIT = 40_000`
- `_to_python(value)` — `value.tolist()` when that attribute is callable, else `value`. Registered
  as the `py` filter.
- `build_environment(search_path) -> Environment` — settings above, `FileSystemLoader(search_path)`,
  registers `py`.
- `resolve_template_path(value) -> Path` — the two-step resolution above; raises `ValueError`.
- `load_template(path) -> Template` — builds the environment and `get_template(path.name)`, wrapping
  `TemplateSyntaxError` in a `ValueError` that carries the file and line number.
- `render(template, context, max_length=SLACK_TEXT_LIMIT) -> str` — renders, strips, truncates with
  a `… (truncated)` marker. Lets `TemplateError` propagate.
- `build_batch_context(result_batch, **extra) -> dict` — the table above.

Truncation matters: a template that loops over every record without a slice will blow Slack's cap on
a large batch, and the API error would arrive as an opaque failure.

### Changes to `src/hyrax_alerts/writers/slack_writer.py`

1. `__init__`: after `max_object_ids`, resolve and compile —
   `self.template = load_template(resolve_template_path(value)) if value else None`. A bad or
   missing template fails at startup, alongside the existing `slack_token` / `channel`
   `ValueError`s, rather than on the first batch an hour into a run.
2. New `_build_message(self, result_batch) -> str`: returns `_format_batch_summary(...)` when
   `self.template is None`; otherwise renders, logging a warning and falling back to
   `_format_batch_summary` on failure. A notification that renders wrong should not abort alert
   processing — and `write_batch` re-raises any writer exception as a `RuntimeError` that kills the
   whole run, so an uncaught render error would do exactly that.

   **Correction found during implementation:** this catches `Exception`, not `jinja2.TemplateError`
   as originally specified. Jinja raises `TemplateError` only for *its own* failures (syntax,
   undefined values); an exception raised by an expression *inside* a template propagates with its
   original type. A test template of `{{ 1 / 0 }}` escaped the narrow catch as `ZeroDivisionError`
   and would have killed the run — exactly the failure mode the fallback exists to prevent.
3. `write()`: call `_build_message`, and **skip the post entirely when the rendered text is blank.**
   `_run_writer` calls `write()` even for an empty `filtered_results`, so this lets
   `{% if count > 0 %}…{% endif %}` suppress empty-batch Slack spam — a real win over the previous
   unconditional "received a batch with no objects" message.
4. Replace the `print()` warning with `logger.warning`. The `TODO` there was stale —
   `hyrax_alerts/logging_utils.py`'s `get_logger` exists and is the pattern used by
   `base_writer.py` and `writer_utils.py`.
5. Expand the class docstring with the config key, the resolution rules, and the context table.

`_format_batch_summary` stays exactly as it is — it is both the default and the fallback, and it is
directly unit-tested.

### Config and packaging

- `pyproject.toml`: add `"jinja2"` to `dependencies`. It is already installed transitively and
  unconditionally (torch, bokeh, `hats`), so this costs nothing at install time, but a direct
  import must be a declared dependency.
- `docs/pre_executed/basic_test_config.toml`: extend the commented-out `to_slack_0` block to
  reference a bundled template by name.
- `default_config.toml` needs no change — it has no `[hyrax_alerts.writers]` section at all, and
  Hyrax's `_validate_runtime_config` only warns on keys lacking a default.

Target config:

```toml
[hyrax_alerts.writers.to_slack_0]
writer_class = "HyraxAlertsSlackWriter"
slack_token = "xoxb-your-token-here"
channel = "#hyrax-alerts"
max_object_ids = 10
# A bundled example by name, or a path to your own .jinja file:
#   message_template = "./my_templates/nightly_summary.jinja"
message_template = "detailed.jinja"
```

And `message_templates/detailed.jinja`:

```jinja
{# Context: records, count, object_ids, max_object_ids. #}
{# Model output lives under record['__hyrax_result']; the `py` filter #}
{# converts numpy scalars and arrays to plain Python for display.     #}
{% if count > 0 %}
:satellite_antenna: *{{ count }}* alert{{ '' if count == 1 else 's' }}
{% for record in records[:max_object_ids] %}
• `{{ record.object_id }}` — {{ record['__hyrax_result']['data'] | py }}
{% endfor %}
{% if count > max_object_ids %}
_…and {{ count - max_object_ids }} more_
{% endif %}
{% endif %}
```

## Tests

Follow the existing style: plain functions with docstrings (ruff `D103` is enforced), no test
classes, `MagicMock` only for the Slack SDK client, `tmp_path` / `caplog` builtins.

**New `tests/hyrax_alerts/test_template_utils.py`:**
- `py` filter converts a numpy scalar and a numpy array; passes plain values through.
- `ChainableUndefined` renders a missing nested key as empty rather than raising.
- `resolve_template_path`: a `tmp_path` file, a bare bundled name, and a miss (raises `ValueError`
  whose message lists the bundled names).
- `load_template` raises `ValueError` on a syntax error, with the filename in the message.
- `{% extends "default.jinja" %}` from a `tmp_path` template resolves against the bundled dir.
- `render` truncates past `max_length`.
- `build_batch_context` keys, including a record with no `object_id`.
- Every bundled `*.jinja` compiles and renders against a sample batch — cheap guard against
  shipping a broken example.
- `default.jinja` output equals `_format_batch_summary` for empty, single, multi, and
  over-`max_object_ids` batches.

**Additions to `tests/hyrax_alerts/test_slack_writer.py`** (reusing `_valid_config` and `_records`):
- A `tmp_path` template shows up in `chat_postMessage(text=...)`.
- A bundled name (`"detailed.jinja"`) resolves and posts.
- No template configured → output still matches `_format_batch_summary`.
- Missing/invalid template raises `ValueError` at construction.
- A template that raises at render time (e.g. `{{ 1 / 0 }}`) → falls back to the default summary,
  logs a warning, does not raise.
- A template rendering blank → `chat_postMessage` is never called.

## Verification

```bash
python -m pytest tests/hyrax_alerts/test_slack_writer.py \
    tests/hyrax_alerts/test_template_utils.py -v
python -m pytest          # full suite; note --doctest-modules runs over src/,
                          # so use `.. code-block::` in new docstrings, not `>>>`
ruff check src tests && ruff format --check src tests
```

Confirm the templates actually ship (they must be git-tracked first):

```bash
git add src/hyrax_alerts/message_templates/
uv build --wheel --out-dir /tmp/wheeltest .
unzip -l /tmp/wheeltest/hyrax_alerts-*.whl | grep jinja   # expect both template files
```

Manual smoke test with no network, exercising the real config path:

```python
from unittest.mock import MagicMock
from hyrax_alerts.writers.slack_writer import HyraxAlertsSlackWriter

writer = HyraxAlertsSlackWriter({
    "slack_token": "xoxb-test", "channel": "#test",
    "message_template": "detailed.jinja",
})
writer.client = MagicMock()
writer.write([{"object_id": "a", "__hyrax_result": {"data": 0.97}}])
print(writer.client.chat_postMessage.call_args.kwargs["text"])
```

End-to-end against a real workspace is optional and requires a token; the `MagicMock` client
covers the wiring, which is the pattern `test_slack_writer.py` already established.
