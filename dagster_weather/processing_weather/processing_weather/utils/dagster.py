from types import ModuleType
from typing import Iterable, Sequence, Union, cast

from dagster import JobDefinition
from dagster._core.definitions.load_assets_from_modules import (
    find_objects_in_module_of_types,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)


def load_jobs_from_modules(modules: Iterable[ModuleType]) -> Sequence[JobDefinition]:
    jobs = []
    for module in modules:
        for job in find_objects_in_module_of_types(
            module, (JobDefinition, UnresolvedAssetJobDefinition)
        ):
            job = cast(Union[JobDefinition, UnresolvedAssetJobDefinition], job)
            jobs.append(job)
    return jobs
