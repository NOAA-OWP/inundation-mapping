from typing import Tuple, Union

import arviz as az
import numpy as np
import pandas as pd
import pymc as pm
from pytensor.tensor import TensorVariable
from scipy.stats import weibull_min


def get_fim_probability_distributions() -> Tuple[weibull_min, weibull_min, weibull_min]:
    """
    Gets default weibull distributions

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


def bayesian_update_for_channel_manning_roughness(data: Union[list, np.array]) -> Tuple[float, float, float]:
    """
    Bayesian update for the default channel manning roughness likelihood

    Parameters
    ----------
    data : Union[list, np.array]
        Observed data used to update the prior distribution for the scale point estimate

    Returns
    -------
    Tuple[float, float, float]
        Parameter values for shape, scale, and location

    """
    rng = 0
    loc = 2

    # Distribution for the scale point estimate
    def g_dist(
        alpha: TensorVariable, beta: TensorVariable, shift: TensorVariable, size: TensorVariable
    ) -> TensorVariable:
        return pm.Gamma.dist(alpha=alpha, beta=beta, size=size) + shift

    # Distribution for the likelihood
    def w_dist(
        alpha: TensorVariable, beta: TensorVariable, shift: TensorVariable, size: TensorVariable
    ) -> TensorVariable:
        return pm.Weibull.dist(alpha=alpha, beta=beta, size=size) + shift

    # Run bayesian update
    with pm.Model() as m:
        alpha = 2
        beta = 1 / 0.75
        shift = loc
        scale = pm.CustomDist(
            "Posterior Scale Estimate", alpha, beta, shift, dist=g_dist, signature="(),()->()"
        )

        # Distribution for the shape point estimate
        shape = pm.DiscreteUniform("Posterior Shape Estimate", lower=1, upper=4)

        # Likelihood using the Weibull distribution
        observed_data = pm.CustomDist(
            "custom_dist2", shape, scale, 1.5, dist=w_dist, signature="(),()->()", observed=data
        )

        # Inference (e.g., using NUTS sampler)
        trace = pm.sample(1000, tune=1000, chains=4)

        idata = pm.sample_posterior_predictive(trace, extend_inferencedata=False, random_seed=rng)

        print(observed_data, idata, m)

    # Summarize and get shape and scale estimate
    az.summary(trace, hdi_prob=0.90)

    updated_scale = trace.mean()['posterior']['Posterior Scale Estimate']
    updated_shape = trace.mean()['posterior']['Posterior Shape Estimate']

    return updated_shape, updated_scale / 100, loc


def bayesian_update_for_overbank_manning_roughness(data: Union[list, np.array]) -> Tuple[float, float, float]:
    """
    Bayesian update for the default overbank manning roughness likelihood

    Parameters
    ----------
    data : Union[list, np.array]
        Observed data used to update the prior distribution for the scale point estimate

    Returns
    -------
    Tuple[float, float, float]
        Parameter values for shape, scale, and location

    """
    rng = 0
    loc = 5

    # Distribution for the scale point estimate
    def g_dist(
        alpha: TensorVariable, beta: TensorVariable, shift: TensorVariable, size: TensorVariable
    ) -> TensorVariable:
        return pm.Gamma.dist(alpha=alpha, beta=beta, size=size) + shift

    # Distribution for the likelihood
    def w_dist(
        alpha: TensorVariable, beta: TensorVariable, shift: TensorVariable, size: TensorVariable
    ) -> TensorVariable:
        return pm.Weibull.dist(alpha=alpha, beta=beta, size=size) + shift

    with pm.Model() as m:
        alpha = 2.5
        beta = 1 / 0.75
        shift = 4
        scale = pm.CustomDist("custom_dist", alpha, beta, shift, dist=g_dist, signature="(),()->()")

        # Distribution for the shape point estimate
        shape = pm.DiscreteUniform("shape", lower=1, upper=5)

        # Likelihood using the Weibull distribution
        observed_data = pm.CustomDist(
            "custom_dist2", shape, scale, 5, dist=w_dist, signature="(),()->()", observed=data
        )

        # Inference (e.g., using NUTS sampler)
        trace = pm.sample(1000, tune=1000, chains=4)

        idata = pm.sample_posterior_predictive(trace, extend_inferencedata=True, random_seed=rng)

        print(observed_data, idata, m)

    # Summarize and get shape and scale estimate
    az.summary(trace, hdi_prob=0.90)

    updated_scale = trace.mean()['posterior']['custom_dist']
    updated_shape = trace.mean()['posterior']['shape']

    return updated_shape, updated_scale / 100, loc


def bayesian_update_for_slope_adjustment(data: Union[list, np.array]) -> Tuple[float, float, float]:
    """
    Bayesian update for the default slope adjustment likelihood

    Parameters
    ----------
    data : Union[list, np.array]
        Observed data used to update the prior distribution for the scale point estimate

    Returns
    -------
    Tuple[float, float, float]
        Parameter values for shape, scale, and location

    """
    rng = 0
    loc = -0.1

    # Distribution for the scale point estimate
    def g_dist(
        alpha: TensorVariable, beta: TensorVariable, shift: TensorVariable, size: TensorVariable
    ) -> TensorVariable:
        return pm.Gamma.dist(alpha=alpha, beta=beta, size=size) + shift

    # Distribution for the likelihood
    def w_dist(
        alpha: TensorVariable, beta: TensorVariable, shift: TensorVariable, size: TensorVariable
    ) -> TensorVariable:
        return pm.Weibull.dist(alpha=alpha, beta=beta, size=size) + shift

    with pm.Model() as m:
        alpha = 2.5
        beta = 1 / 0.75
        shift = 0.5
        scale = pm.CustomDist("custom_dist", alpha, beta, shift, dist=g_dist, signature="(),()->()")

        # Distribution for the shape point estimate
        shape = pm.DiscreteUniform("shape", lower=1, upper=5)

        # Likelihood using the Weibull distribution

        observed_data = pm.CustomDist(
            "custom_dist2", shape, scale, 0.75, dist=w_dist, signature="(),()->()", observed=data
        )

        # Inference (e.g., using NUTS sampler)
        trace = pm.sample(1000, tune=1000, chains=4)

        idata = pm.sample_posterior_predictive(trace, extend_inferencedata=True, random_seed=rng)

        print(observed_data, idata, m)

    # Summarize and get shape and scale estimate
    az.summary(trace, hdi_prob=0.90)

    updated_scale = trace.mean()['posterior']['custom_dist']
    updated_shape = trace.mean()['posterior']['shape']

    return updated_shape, updated_scale / 100, loc


def run_bayesian_updates(
    channel_manning_data: Union[list, np.ndarray],
    overbank_manning_data: Union[list, np.ndarray],
    slope_adjustment_data: Union[list, np.ndarray],
) -> pd.DataFrame:
    """
    Bayesian update for the default channel manning roughness, overbank manning roughness, and slope
    adjustment distributions

    Parameters
    ----------
    channel_manning_data : Union[list, np.ndarray]
        Observed data used to update the prior distribution for the scale point estimate
    overbank_manning_data : Union[list, np.ndarray]
        Observed data used to update the prior distribution for the scale point estimate
    slope_adjustment_data : Union[list, np.ndarray]
        Observed data used to update the prior distribution for the scale point estimate

    Returns
    -------
    pd.DataFrame
        DataFrame containing shape, scale, and location parameters
    """

    parameter_names = ["channel_mannning_roughness", "overbank_manning_roughness", "slope_adjustment"]

    n_shp, n_scale, n_loc = bayesian_update_for_channel_manning_roughness(channel_manning_data)
    on_shp, on_scale, on_loc = bayesian_update_for_overbank_manning_roughness(overbank_manning_data)
    slp_shp, slp_scale, slp_loc = bayesian_update_for_slope_adjustment(slope_adjustment_data)

    posterior_distribution_parameters = pd.DataFrame(
        {
            "parameter_name": parameter_names,
            "shape": [n_shp, n_scale, slp_shp],
            "scale": [n_scale, on_scale, slp_scale],
            "loc": [n_loc, on_loc, slp_loc],
        }
    )

    return posterior_distribution_parameters


if __name__ == "__main__":

    # Scaled by 100
    channel_manning_data = [9, 10, 8, 7, 8, 10, 10, 10, 10, 10, 10, 10, 10]
    overbank_manning_data = [15, 14, 12, 13, 11, 10, 11, 12, 15, 14, 14, 14, 13]

    # Scaled by 100 translated where 0 is represented by approx 1.75
    slope_adjustment_data = [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3]

    df = run_bayesian_updates(
        channel_manning_data=channel_manning_data,
        overbank_manning_data=overbank_manning_data,
        slope_adjustment_data=slope_adjustment_data,
    )
