from urllib.parse import quote

from hyrax_alerts.logging_utils import get_logger
from hyrax_alerts.writers.base_writer import HyraxAlertsBaseWriter
from hyrax_alerts.writers.serialization_utils import to_jsonable

# NOTE: This writer annotates *existing* SkyPortal sources; it never creates
# them. An alert for an object SkyPortal has not heard of produces an error that
# is logged and skipped, rather than aborting the run.

# NOTE: SkyPortal reports application-level failures as HTTP 400 with a JSON body
# carrying a "status" field, so success is determined from the body rather than
# from response.ok alone.

# NOTE: requests.Session is not documented as thread-safe. It is safe here
# because write_batch submits exactly one future per writer per batch and waits
# on all of them before the next batch, so only one worker thread is ever inside
# a given writer instance's write().

# NOTE: The requests import is deferred into _load_requests_module so that
# hyrax_alerts.writers can import this module - and therefore register this
# writer - without the `skyportal` extra installed.

logger = get_logger(__name__)

# Where merge_batch puts the model output on every record.
RESULT_FIELD = "__hyrax_result"

# Which record field holds the SkyPortal object id.
DEFAULT_OBJ_ID_FIELD = "object_id"

# Seconds to wait on each SkyPortal request.
DEFAULT_TIMEOUT_SECONDS = 30

# SkyPortal enforces one annotation per (obj_id, origin). Re-seeing an object in
# a stream is normal, so this is handled rather than treated as an error.
DUPLICATE_ANNOTATION_MARKER = "Annotation already exists"

SKYPORTAL_IMPORT_ERROR = (
    "HyraxAlertsSkyPortalWriter requires requests. Install it with: pip install 'hyrax_alerts[skyportal]'"
)


def _load_requests_module():
    """Import and return the ``requests`` module.

    Returns
    -------
    module
        The ``requests`` module.

    Raises
    ------
    ImportError
        If requests is not installed, with a message naming the extra that
        provides it.
    """
    try:
        import requests
    except ImportError as error:
        raise ImportError(SKYPORTAL_IMPORT_ERROR) from error
    return requests


def _annotation_data(record: dict) -> dict:
    """Build the ``data`` payload for one annotation.

    Only the model output is posted. The input alert data already lives in
    SkyPortal, so the annotation carries just what hyrax added.

    Parameters
    ----------
    record : dict
        One alert record from a result batch.

    Returns
    -------
    dict
        The contents of the record's ``__hyrax_result`` field, coerced to
        JSON-serializable values.
    """
    return to_jsonable(dict(record.get(RESULT_FIELD, {})))


def _response_status(response) -> tuple[bool, str]:
    """Interpret a SkyPortal API response.

    SkyPortal signals failure with a JSON body of
    ``{"status": "error", "message": ...}``, often alongside an HTTP 400, so a
    body that parses is trusted over the status code. A body that does not parse
    falls back to the status code.

    Parameters
    ----------
    response : object
        The response returned by ``requests``.

    Returns
    -------
    tuple[bool, str]
        ``(succeeded, message)``, where ``message`` is SkyPortal's error text
        when it provided one.
    """
    try:
        body = response.json()
    except Exception:
        body = None

    if isinstance(body, dict) and "status" in body:
        return body["status"] == "success", str(body.get("message", ""))

    return bool(getattr(response, "ok", False)), f"HTTP {getattr(response, 'status_code', '?')}"


def _response_data(response):
    """Return the ``data`` payload of a successful SkyPortal response.

    Parameters
    ----------
    response : object
        The response returned by ``requests``.

    Returns
    -------
    object
        The response body's ``data`` field, or ``None`` when the body could not
        be parsed or carried no such field.
    """
    try:
        body = response.json()
    except Exception:
        return None
    return body.get("data") if isinstance(body, dict) else None


def _is_duplicate_annotation(message: str) -> bool:
    """Return whether a SkyPortal error message is the unique-origin conflict.

    Parameters
    ----------
    message : str
        The error message from a SkyPortal response.

    Returns
    -------
    bool
        ``True`` when the object already carries an annotation with this origin.
    """
    return DUPLICATE_ANNOTATION_MARKER in message


class HyraxAlertsSkyPortalWriter(HyraxAlertsBaseWriter):
    """Writer class for annotating existing SkyPortal sources with model output.

    Requires the ``skyportal`` extra: ``pip install 'hyrax_alerts[skyportal]'``.

    One annotation is posted per alert, to
    ``POST {base_url}/api/sources/{obj_id}/annotations``, carrying the record's
    model output (``__hyrax_result``) as the annotation ``data``. This writer
    annotates objects SkyPortal already knows about; it never creates sources.

    SkyPortal allows only one annotation per ``(object, origin)`` pair. When an
    object already carries an annotation with this writer's ``origin``, the
    existing annotation is updated in place so the latest model output wins. Set
    ``update_existing = false`` to leave the first annotation untouched instead.

    Failures are logged rather than raised: an unreachable instance, an unknown
    object, or a rejected annotation should not abort a long-running run.

    Required configuration keys:

    - ``base_url``: the SkyPortal instance, e.g. ``https://skyportal.example.org``.
    - ``token``: a SkyPortal API token, sent as ``Authorization: token <token>``.
    - ``origin``: the annotation origin, which identifies this writer's
      annotations within SkyPortal.

    Optional configuration keys:

    - ``group_ids``: the groups the annotation is visible to. When omitted,
      SkyPortal defaults to the token owner's accessible groups.
    - ``obj_id_field``: which record field holds the SkyPortal object id
      (default ``"object_id"``). Records without that field are skipped.
    - ``update_existing``: whether to update an object's existing annotation for
      this origin (default ``true``).
    - ``timeout``: seconds to wait on each request (default ``30``).

    Example configuration:

    .. code-block:: toml

        [hyrax_alerts.writers.to_skyportal_0]
        writer_class = "HyraxAlertsSkyPortalWriter"
        base_url = "https://skyportal.example.org"
        token = "your-skyportal-api-token"
        origin = "hyrax-alerts"
        group_ids = [1]
    """

    def __init__(self, config):
        super().__init__(config)

        base_url = config.get("base_url")
        if not isinstance(base_url, str) or not base_url.strip():
            raise ValueError("HyraxAlertsSkyPortalWriter requires a non-empty string 'base_url'.")
        self.base_url = base_url.rstrip("/")

        self.token = config.get("token")
        if not isinstance(self.token, str) or not self.token.strip():
            raise ValueError("HyraxAlertsSkyPortalWriter requires a non-empty string 'token'.")

        self.origin = config.get("origin")
        if not isinstance(self.origin, str) or not self.origin.strip():
            raise ValueError("HyraxAlertsSkyPortalWriter requires a non-empty string 'origin'.")

        self.group_ids = config.get("group_ids")
        self.obj_id_field = config.get("obj_id_field", DEFAULT_OBJ_ID_FIELD)
        self.update_existing = config.get("update_existing", True)
        self.timeout = config.get("timeout", DEFAULT_TIMEOUT_SECONDS)

        requests_module = _load_requests_module()
        self.session = requests_module.Session()
        self.session.headers.update({"Authorization": f"token {self.token}"})

    def write(self, result_batch: list[dict]):
        """Post one annotation per record to the configured SkyPortal instance.

        Parameters
        ----------
        result_batch : list[dict]
            A batch of post-processed, post-filtered alert records.

        Returns
        -------
        None
        """
        for record in result_batch:
            obj_id = record.get(self.obj_id_field)
            if obj_id is None:
                logger.warning(f"Skipping a record with no '{self.obj_id_field}' field.")
                continue

            payload = {"origin": self.origin, "data": _annotation_data(record)}
            if self.group_ids:
                payload["group_ids"] = self.group_ids

            self._post_annotation(str(obj_id), payload)

    def _annotations_url(self, obj_id: str) -> str:
        """Return the annotations endpoint for one object.

        Parameters
        ----------
        obj_id : str
            The SkyPortal object id.

        Returns
        -------
        str
            The full annotations URL for that object.
        """
        # Well-formed object ids are unchanged by quoting; it is here so that a
        # malformed id yields a clean error from SkyPortal rather than a
        # path-traversal-shaped URL.
        return f"{self.base_url}/api/sources/{quote(obj_id, safe='')}/annotations"

    def _post_annotation(self, obj_id: str, payload: dict):
        """Post one annotation, logging and swallowing every failure.

        Parameters
        ----------
        obj_id : str
            The SkyPortal object id to annotate.
        payload : dict
            The annotation body, with ``origin`` and ``data`` entries.

        Returns
        -------
        None
        """
        try:
            response = self.session.post(self._annotations_url(obj_id), json=payload, timeout=self.timeout)
        except Exception as error:
            logger.error(f"Failed to reach SkyPortal to annotate object {obj_id}: {error}")
            return

        succeeded, message = _response_status(response)
        if succeeded:
            return

        if _is_duplicate_annotation(message):
            if self.update_existing:
                self._update_annotation(obj_id, payload)
            else:
                logger.info(f"SkyPortal already has a '{self.origin}' annotation for object {obj_id}.")
            return

        logger.error(f"SkyPortal rejected the annotation for object {obj_id}: {message}")

    def _update_annotation(self, obj_id: str, payload: dict):
        """Update the object's existing annotation for this writer's origin.

        Looks up the object's annotations, finds the one matching ``origin``,
        and replaces its contents so the latest model output wins.

        Parameters
        ----------
        obj_id : str
            The SkyPortal object id to update.
        payload : dict
            The annotation body, with ``origin`` and ``data`` entries.

        Returns
        -------
        None
        """
        url = self._annotations_url(obj_id)

        try:
            response = self.session.get(url, timeout=self.timeout)
        except Exception as error:
            logger.error(f"Failed to reach SkyPortal to list annotations for object {obj_id}: {error}")
            return

        succeeded, message = _response_status(response)
        if not succeeded:
            logger.error(f"SkyPortal would not list annotations for object {obj_id}: {message}")
            return

        annotations = _response_data(response) or []
        annotation_id = next(
            (
                annotation.get("id")
                for annotation in annotations
                if isinstance(annotation, dict) and annotation.get("origin") == self.origin
            ),
            None,
        )
        if annotation_id is None:
            logger.error(
                f"SkyPortal reported an existing '{self.origin}' annotation for object "
                f"{obj_id}, but none was found to update."
            )
            return

        try:
            response = self.session.put(f"{url}/{annotation_id}", json=payload, timeout=self.timeout)
        except Exception as error:
            logger.error(f"Failed to reach SkyPortal to update the annotation for object {obj_id}: {error}")
            return

        succeeded, message = _response_status(response)
        if not succeeded:
            logger.error(f"SkyPortal rejected the annotation update for object {obj_id}: {message}")

    def __enter__(self):
        """Enter the context manager for the writer."""
        return self

    def close(self):
        """Close the pooled HTTP connections held by the session."""
        self.session.close()
