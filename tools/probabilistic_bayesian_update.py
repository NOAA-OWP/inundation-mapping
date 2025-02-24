import argparse
from typing import Tuple

from scipy.stats import (
    expon,
    gamma,
    genextreme,
    genpareto,
    gumbel_r,
    kappa4,
    norm,
    pearson3,
    truncexpon,
    weibull_min,
)


def get_fim_probability_distributions(
    posterior_dist: str = None, huc: int = None
) -> Tuple[weibull_min, weibull_min, weibull_min]:
    """
    Gets either bayesian updated distributions or default distributions for respective huc

    Parameters
    ---------
    posterior_dist : str, default=None
        Name of csv file that has posteriod distribution parameters
    huc : int
        Identifier for the huc of interest

    Returns
    -------
    Tuple[gamma, gamma, gamma]
        Gamma distributions for channel Manning roughness overbank Manning roughness, and slope adjustment

    """

    # Default weibull likelihood for channel manning roughness
    channel_dist = weibull_min(c=1.5, scale=0.0367, loc=0.032)

    # Default weibull likelihood for overbank manning roughness
    obank_dist = weibull_min(c=2, scale=0.035, loc=0.09)

    # Default weibull likelihood for slope adjustment
    slope_dist = weibull_min(c=3.1, scale=0.095, loc=-0.01)

    return channel_dist, obank_dist, slope_dist
