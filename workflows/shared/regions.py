"""Region definitions and bash-variable path helpers used by input workflows."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import Mapping


@dataclass(frozen=True)
class Region:
    name: str
    crs: int
    huc_prefix: str | None
    huc: str | None
    dem_env_var: str
    bridge_diff_env_var: str


REGIONS: dict[str, Region] = {
    "conus": Region("conus", 5070, None, None, "input_DEM", "input_bridge_elev_diff"),
    "alaska": Region("alaska", 3338, "19", None, "input_DEM_Alaska", "input_bridge_elev_diff_alaska"),
    "guam": Region("guam", 6637, None, "22010000", "input_DEM_Guam", "input_bridge_elev_diff_guam"),
    "american_samoa": Region(
        "american_samoa",
        32702,
        None,
        "22030001",
        "input_DEM_AmericanSamoa",
        "input_bridge_elev_diff_americansamoa",
    ),
}

EXTRA_ALASKA_DEM_ENV_VAR = "OSM_DEM_DIFF_EXTRA_DEM_DIRS_ALASKA"


def get_region(region: str | Region) -> Region:
    if isinstance(region, Region):
        return region
    try:
        return REGIONS[region.lower()]
    except KeyError as exc:
        raise ValueError(f"Unknown region: {region}") from exc


def region_for_huc(huc: str | int) -> Region:
    huc_text = str(huc)
    for region in REGIONS.values():
        if region.huc is not None and huc_text == region.huc:
            return region
        if region.huc_prefix is not None and huc_text.startswith(region.huc_prefix):
            return region
    return REGIONS["conus"]


def _expand_path(value: str, variables: Mapping[str, str]) -> str:
    substitutions = {**os.environ, **variables}
    return Template(value).safe_substitute(substitutions)


def dem_directories_for_region(region: str | Region, bash_variables: Mapping[str, str]) -> list[Path]:
    """Return DEM input directories for `-d`; Alaska may include configured extra folders."""
    region_def = get_region(region)
    dem_path = bash_variables.get(region_def.dem_env_var)
    if not dem_path:
        raise ValueError(f"Missing {region_def.dem_env_var} in bash variables")

    directories = [Path(_expand_path(dem_path, bash_variables)).parent]
    if region_def.name == "alaska":
        # Space- or comma-delimited extra folders, e.g. the North Alaska IfSAR DEMs.
        extras = bash_variables.get(EXTRA_ALASKA_DEM_ENV_VAR, "").replace(",", " ").split()
        directories.extend(Path(_expand_path(item, bash_variables)) for item in extras)

    unique_directories: list[Path] = []
    for directory in directories:
        if directory not in unique_directories:
            unique_directories.append(directory)
    return unique_directories


def bridge_diff_variable_for_region(region: str | Region) -> str:
    return get_region(region).bridge_diff_env_var


def region_crs(region: str | Region) -> int:
    return get_region(region).crs
