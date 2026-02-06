from typing import Any

import orjson
from fastapi.responses import JSONResponse


class BackendResponse(JSONResponse):
    """JSON response using orjson with sorted keys and indent."""

    def render(self, content: Any) -> bytes:  # noqa: ANN401
        """Renders the given content to JSON bytes."""
        return orjson.dumps(content, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)
