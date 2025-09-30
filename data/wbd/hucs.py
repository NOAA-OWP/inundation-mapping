"""
Manage Hydrologic Unit Codes
"""

from __future__ import annotations

import os
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Set

import fiona
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm


try:
    import pygeohydro as gh
except ImportError:
    pygeohydro_installed = False
    pass
else:
    pygeohydro_installed = True

# Load environment variables
src_dir = os.getenv('srcDir')
load_dotenv(f'{src_dir}/bash_variables.env')
input_WBD = os.getenv('input_WBD_gdb')


class Huc:
    """
    Manage Hydrologic Unit Codes

    Parameters
    ----------
    huc : str
        The Hydrologic Unit Code (HUC) to manage

    Attributes
    ----------
    huc : str
        The Hydrologic Unit Code (HUC) to manage
    huc_level : int
        The level of the HUC

    Methods
    -------
    get_child_hucs(huc_level: int) -> Set of str
        Get the child HUCs for a given HUC level
    get_parent_hucs(huc_level: int) -> Set of str
        Get the parent HUCs for a given HUC level
    is_valid -> bool
        Check if the HUC is valid
    """

    _valid_wbd_sources = {'pygeohydro', 'local'}
    _valid_huc_levels = {6, 8, 10, 12, 14, 16}  # {2, 4, 6, 8, 10, 12, 14, 16} don't have 2 and 4 processed

    def __init__(self, huc: str, wbd_source: str = 'local'):

        self.huc = huc
        self.huc_level = len(huc)
        self._available_hucs = {}
        self._child_hucs = {}
        self._parent_hucs = {}
        self._wbd_source = wbd_source

        if self.huc_level not in self._valid_huc_levels:
            raise ValueError(
                f'{self.huc_level} is not a valid HUC level for HUC, {self.huc}. Choose from {self._valid_huc_levels}'
            )

        if self._wbd_source not in self._valid_wbd_sources:
            raise ValueError(
                f'{wbd_source} is not a valid WBD source for HUC, {self.HUC}. Choose from {self._valid_wbd_sources}'
            )

        if (not pygeohydro_installed) and (self._wbd_source == 'pygeohydro'):
            warnings.warn('pygeohydro is not installed. Using local WBD data instead.')
            self._wbd_source = 'local'

    def _get_huc_pygh(self, huc_level: int) -> Set[str]:
        """
        Get the HUC set for a given HUC level
        """
        if huc_level not in self._valid_huc_levels:
            raise ValueError(f'{huc_level} is not a valid HUC level. Choose from {self._valid_huc_levels}')

        # Get the HUCs for the given HUC level
        hucs = gh.watershed.huc_wb_full(huc_level)[f'huc{huc_level}']

        # Convert the HUCs to a set
        if not isinstance(hucs, pd.Series):
            hucs = hucs.iloc[:, 0]

        return set(hucs.to_list())

    def _get_huc_local(self, huc_level: int) -> Set[str]:
        """
        Get the HUC set for a given HUC level
        """
        if huc_level not in self._valid_huc_levels:
            raise ValueError(f'{huc_level} is not a valid HUC level. Choose from {self._valid_huc_levels}')

        # TODO: This is a temporary fix. Need to remove this hardcoding
        input_WBD = f'/data/inputs/wbd/WBD_National_EPSG_5070_WBDHU{huc_level}_clip_dem_domain.gpkg'

        # instead of using geopandas use fiona to just read the HUC level columns
        with fiona.open(input_WBD) as src:
            hucs = [f['properties'][f'HUC{huc_level}'] for f in src]

        return set(hucs)

    def _get_huc(self, huc_level: int) -> Set[str]:
        """
        Get the HUC set for a given HUC level
        """
        if self._wbd_source == 'pygeohydro':
            return self._get_huc_pygh(huc_level)
        elif self._wbd_source == 'local':
            return self._get_huc_local(huc_level)

    def _set_available_hucs(self, huc_level: int):
        """Set the child HUCs for a given HUC level"""
        if huc_level not in self._available_hucs:
            self._available_hucs[huc_level] = self._get_huc(huc_level)

    def get_child_hucs(self, huc_level: int, as_list: bool = False) -> Set[str] | List[str]:
        """Get the child HUCs for a given HUC level"""
        self._set_available_hucs(huc_level)
        if huc_level not in self._child_hucs:
            self._child_hucs[huc_level] = {
                h for h in self._available_hucs[huc_level] if h[: self.huc_level] == self.huc
            }
        if as_list:
            return sorted(self._child_hucs[huc_level])
        return self._child_hucs[huc_level]

    def get_parent_hucs(self, huc_level: int, as_list: bool = False) -> Set[str] | List[str]:
        """Get the parent HUCs for a given HUC level"""
        self._set_available_hucs(huc_level)
        if huc_level not in self._parent_hucs:
            self._parent_hucs[huc_level] = {
                h for h in self._available_hucs[huc_level] if h == self.huc[:huc_level]
            }
        if as_list:
            return sorted(self._parent_hucs[huc_level])
        return self._parent_hucs[huc_level]

    def get_any_hucs(self, huc_level: int, as_list: bool = False) -> Set[str] | List[str]:
        """Get the parent or child HUCs for a given HUC level"""
        if huc_level > self.huc_level:
            return self.get_child_hucs(huc_level, as_list)
        elif huc_level < self.huc_level:
            return self.get_parent_hucs(huc_level, as_list)
        else:
            return {self.huc}

    @property
    def is_valid(self) -> bool:
        """Check if the HUC is valid"""
        self._set_available_hucs(self.huc_level)
        return self.huc in self._available_hucs[self.huc_level]

    def __str__(self):
        return self.huc

    def __repr__(self):
        return self.huc

    def __eq__(self, other: Huc):
        return self.huc == other.huc

    def __lt__(self, other: Huc):
        return self.huc < other.huc

    def __le__(self, other: Huc):
        return self.huc <= other.huc

    def __gt__(self, other: Huc):
        return self.huc > other.huc

    def __ge__(self, other: Huc):
        return self.huc >= other.huc

    def __ne__(self, other: Huc):
        return self.huc != other.huc

    def __hash__(self):
        return hash(self.huc)

    def __contains__(self, other: Huc):
        return other.huc.startswith(self.huc)

    def __len__(self):
        return len(self.huc)

    def __getitem__(self, key: int):
        return self.huc[key]

    def __iter__(self):
        return iter(self.huc)

    def __reversed__(self):
        return reversed(self.huc)

    def __add__(self, other: Huc):
        return Huc(self.huc + other.huc)

    def __sub__(self, other: Huc):
        return Huc(self.huc[: -len(other.huc)])

    def __truediv__(self, other: Huc):
        return Huc(self.huc[: -len(other.huc)])

    def __floordiv__(self, other: Huc):
        return Huc(self.huc[: -len(other.huc)])

    def __mod__(self, other: Huc):
        return Huc(self.huc[: -len(other.huc)])

    def __mul__(self, other: int):
        return Huc(self.huc * other)

    def __rmul__(self, other: int):
        return Huc(self.huc * other)


class HucList:
    """
    Manage a list of Hydrologic Unit Codes
    """

    def __init__(self, hucs: Set[str | Huc] | List[str | Huc] | pd.Series[str | Huc]):
        # Convert HUCs to set
        self.hucs = {h if isinstance(h, Huc) else Huc(h) for h in hucs}
        self.huc_level = {h.huc_level for h in self.hucs}
        self._child_hucs = {}
        self._parent_hucs = {}
        self._available_hucs = {}

    def _get_huc(self, huc_level: int) -> Set[str]:
        """
        Get the HUC set for a given HUC level
        """
        huc = self.hucs.pop()
        hucs = huc._get_huc(huc_level)
        self.hucs.add(huc)

    # set the available HUCs at the HUCList level to avoid redundant calls
    def _set_available_hucs(self, huc_level: int):
        """Set the child HUCs for a given HUC level"""
        if huc_level not in self._available_hucs:
            self._available_hucs[huc_level] = self._get_huc(huc_level)

    @staticmethod
    def _dict_to_series(hucs: Dict[str, Set[str]]) -> pd.Series[pd.StringDtype]:
        """Convert a dictionary of HUCs to a pandas Series"""
        return pd.Series(hucs).explode().astype(pd.StringDtype())

    ## Child HUCs ##
    def _get_child_hucs_serial(self, huc_level: int, verbose: bool) -> Dict[str, Set[str]]:
        """Serially get child HUCs for a given level."""
        return {
            str(h): h.get_child_hucs(huc_level)
            for h in tqdm(self.hucs, desc=f'Getting child HUC{huc_level}s', disable=(not verbose))
        }

    @staticmethod
    def _fetch_child_hucs(h, huc_level: int):
        """Helper function to fetch child HUCs for parallel processing"""
        return str(h), h.get_child_hucs(huc_level)

    def _get_child_hucs_parallel(self, huc_level: int, verbose: bool, n_jobs: int) -> Dict[str, Set[str]]:
        """Parallelized version of getting child HUCs using ProcessPoolExecutor."""
        hucs = {}
        with ProcessPoolExecutor(max_workers=n_jobs) as executor:
            futures = {executor.submit(self._fetch_child_hucs, h, huc_level): h for h in self.hucs}

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=f'Getting child HUC{huc_level}s with {n_jobs} workers',
                disable=(not verbose),
            ):
                key, value = future.result()
                hucs[key] = value

        return hucs

    def get_child_hucs(
        self, huc_level: int, as_series: bool = True, verbose: bool = False, n_jobs: int = 1
    ) -> Set[str] | pd.Series:
        """Get the child HUCs for a given level, optionally using parallel execution."""
        self._set_available_hucs(huc_level)

        if n_jobs > 1 and len(self.hucs) > 1:
            hucs = self._get_child_hucs_parallel(huc_level, verbose, n_jobs)
        else:
            hucs = self._get_child_hucs_serial(huc_level, verbose)

        return HucList._dict_to_series(hucs) if as_series else hucs

    ## Parent HUCs ##
    def _get_parent_hucs_serial(self, huc_level: int, verbose: bool) -> Dict[str, Set[str]]:
        """Serially get parent HUCs for a given level."""
        return {
            str(h): h.get_parent_hucs(huc_level)
            for h in tqdm(self.hucs, desc=f'Getting parent HUC{huc_level}s', disable=(not verbose))
        }

    @staticmethod
    def _fetch_parent_hucs(h, huc_level: int):
        """Helper function to fetch parent HUCs for parallel processing"""
        return str(h), h.get_parent_hucs(huc_level)

    def _get_parent_hucs_parallel(self, huc_level: int, verbose: bool, n_jobs: int) -> Dict[str, Set[str]]:
        """Parallelized version of getting parent HUCs using ProcessPoolExecutor."""
        hucs = {}
        with ProcessPoolExecutor(max_workers=n_jobs) as executor:
            futures = {executor.submit(self._fetch_parent_hucs, h, huc_level): h for h in self.hucs}

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=f'Getting parent HUC{huc_level}s with {n_jobs} workers',
                disable=(not verbose),
            ):
                key, value = future.result()
                hucs[key] = value

        return hucs

    def get_parent_hucs(
        self, huc_level: int, as_series: bool = True, verbose: bool = False, n_jobs: int = 1
    ) -> Set[str] | pd.Series:
        """Get the parent HUCs for a given level, optionally using parallel execution."""
        self._set_available_hucs(huc_level)

        if n_jobs > 1 and len(self.hucs) > 1:
            hucs = self._get_parent_hucs_parallel(huc_level, verbose, n_jobs)
        else:
            hucs = self._get_parent_hucs_serial(huc_level, verbose)

        return HucList._dict_to_series(hucs) if as_series else hucs

    ## Any HUCs ##
    def _get_any_hucs_serial(self, huc_level: int, verbose: bool) -> Dict[str, Set[str]]:
        """Serially get HUCs for a given level (no parallelization)."""
        return {
            str(h): h.get_any_hucs(huc_level)
            for h in tqdm(self.hucs, desc=f'Getting HUC{huc_level}s', disable=(not verbose))
        }

    @staticmethod
    def _fetch_any_hucs(h, huc_level: int):
        """Helper function to fetch HUCs for parallel processing"""
        return str(h), h.get_any_hucs(huc_level)

    def _get_any_hucs_parallel(self, huc_level: int, verbose: bool, n_jobs: int) -> Dict[str, Set[str]]:
        """Parallelized version of getting HUCs using ProcessPoolExecutor."""

        hucs = {}
        with ProcessPoolExecutor(max_workers=n_jobs) as executor:
            futures = {executor.submit(self._fetch_any_hucs, h, huc_level): h for h in self.hucs}

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=f'Getting HUC{huc_level}s with {n_jobs} workers',
                disable=(not verbose),
            ):
                key, value = future.result()
                hucs[key] = value

        return hucs

    def get_any_hucs(
        self, huc_level: int, as_series: bool = True, verbose: bool = False, n_jobs: int = 1
    ) -> Set[str] | pd.Series:
        """Get the parent or child HUCs for a given HUC level, optionally using parallel execution."""

        # Choose execution mode
        if n_jobs > 1 and len(self.hucs) > 1:
            hucs = self._get_any_hucs_parallel(huc_level, verbose, n_jobs)
        elif n_jobs == 1:
            hucs = self._get_any_hucs_serial(huc_level, verbose)
        else:
            raise ValueError('n_jobs must be greater than or equal to 1')

        return HucList._dict_to_series(hucs) if as_series else hucs

    '''
        def get_child_hucs(
        self, huc_level: int, as_series: bool = True, verbose: bool = False
    ) -> Set[str] | pd.Series[str]:
        """Get the child HUCs for a given HUC level"""

        # check if the available HUCs have been set
        self._set_available_hucs(huc_level)

        # use available hucs to get the child hucs
        if huc_level not in self._child_hucs:
            self._child_hucs[huc_level] = {
                h: h.get_child_hucs(huc_level)
                for h in tqdm(self.hucs, desc=f'Getting child HUC{huc_level}s', disable=(not verbose))
            }
        if as_series:
            return pd.Series({str(h): ch for h, ch in self._child_hucs[huc_level].items()}).explode()
        return self._child_hucs[huc_level]

    def get_parent_hucs(
        self, huc_level: int, as_series: bool = True, verbose: bool = False
    ) -> Set[str] | pd.Series[str]:
        """Get the parent HUCs for a given HUC level"""

        # check if the available HUCs have been set
        self._set_available_hucs(huc_level)

        # use available hucs to get the parent hucs
        if huc_level not in self._parent_hucs:
            self._parent_hucs[huc_level] = {
                h: h.get_parent_hucs(huc_level)
                for h in tqdm(self.hucs, desc=f'Getting parent HUC{huc_level}s', disable=(not verbose))
            }
        if as_series:
            return pd.Series({str(h): ph for h, ph in self._parent_hucs[huc_level].items()}).explode()
        return self._parent_hucs[huc_level]

    def get_any_hucs(self, huc_level: int, as_series: bool = True, verbose: bool = False) -> Set[str] | pd.Series[str]:
        """Get the parent or child HUCs for a given HUC level"""
        hucs = {
            str(h): h.get_any_hucs(huc_level)
            for h in tqdm(self.hucs, desc=f'Getting HUC{huc_level}s', disable=(not verbose))
        }
        if as_series:
            return HucList._dict_to_series(hucs)
        return hucs
    '''

    @classmethod
    def from_huc_list_file(cls, file_path: str):
        """
        Create a HucList from a file
        """
        with open(file_path, 'r') as f:
            hucs = {Huc(line.strip()) for line in f}
        return cls(hucs)

    def to_huc_list_file(self, file_path: str):
        """
        Write the HucList to a file
        """
        # sort
        hucs = sorted(self.hucs)
        with open(file_path, 'w') as f:
            for huc in hucs:
                f.write(f'{huc}\n')

    def __str__(self):
        return ', '.join(str(h) for h in self.hucs)

    def __repr__(self):
        return "HucList <" + ', '.join(repr(h) for h in self.hucs) + ">"

    def __contains__(self, other: Huc):
        return any(other in h for h in self.hucs)

    def __len__(self):
        return len(self.hucs)

    def __iter__(self):
        return iter(self.hucs)

    def __reversed__(self):
        return reversed(self.hucs)

    def __add__(self, other: HucList):
        return HucList(self.hucs | other.hucs)

    def __sub__(self, other: HucList):
        return HucList(self.hucs - other.hucs)

    def __truediv__(self, other: HucList):
        return HucList(self.hucs - other.hucs)

    def __floordiv__(self, other: HucList):
        return HucList(self.hucs - other.hucs)

    def __mod__(self, other: HucList):
        return HucList(self.hucs - other.hucs)

    def __mul__(self, other: int):
        return HucList(self.hucs * other)

    def __rmul__(self, other: int):
        return HucList(self.hucs * other)

    def __eq__(self, other: HucList):
        return self.hucs == other.hucs

    def __ne__(self, other: HucList):
        return self.hucs != other.hucs

    def __lt__(self, other: HucList):
        return self.hucs < other.hucs

    def __le__(self, other: HucList):
        return self.hucs <= other.hucs

    def __gt__(self, other: HucList):
        return self.hucs > other.hucs

    def __ge__(self, other: HucList):
        return self.hucs >= other.hucs

    def __hash__(self):
        return hash(self.hucs)

    def __getitem__(self, key: int):
        return self.hucs[key]

    def __setitem__(self, key: int, value: Huc):
        self.hucs[key] = value


if __name__ == '__main__':

    """
    huc = Huc('10030101')
    print(f'Getting any HUC12s for {huc}')
    print(huc.get_any_hucs(12))
    print(f'Getting any HUC6s for {huc}')
    print(huc.get_any_hucs(6))

    huc = Huc('01010001')
    print(f'Getting child HUC12s for {huc}')
    print(huc.get_child_hucs(12))

    print(f'Getting parent HUC2s for {huc}')
    print(huc.get_parent_hucs(6))
    """
    hucs = [Huc('01010002'), Huc('100301'), Huc('120902'), Huc('120903010101')]

    huc_list = HucList(hucs)
    print(f'Getting child HUC10s for {huc_list}')
    pd.testing.assert_series_equal(
        huc_list.get_child_hucs(10, verbose=True, n_jobs=2).sort_index().sort_values(),
        huc_list.get_child_hucs(10, verbose=True).sort_index().sort_values(),
    )
    print(f'Getting parent HUC6s for {huc_list}')
    pd.testing.assert_series_equal(
        huc_list.get_parent_hucs(6, verbose=True, n_jobs=2).sort_index().sort_values(),
        huc_list.get_parent_hucs(6, verbose=True).sort_index().sort_values(),
    )
    print(f'Getting any HUC8s for {huc_list}')
    pd.testing.assert_series_equal(
        huc_list.get_any_hucs(8, verbose=True, n_jobs=2).sort_index().sort_values(),
        huc_list.get_any_hucs(8, verbose=True).sort_index().sort_values(),
    )
