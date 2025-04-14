from collections.abc import Callable
from typing import Any

from mex.backend.auxiliary.organigram import extracted_organizational_unit
from mex.backend.auxiliary.primary_source import (
    extracted_primary_source_ldap,
    extracted_primary_source_orcid,
    extracted_primary_source_wikidata,
)

startup_tasks: list[Callable[[], Any]] = [
    extracted_primary_source_ldap,
    extracted_primary_source_orcid,
    extracted_primary_source_wikidata,
    extracted_organizational_unit,
]
