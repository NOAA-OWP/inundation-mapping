import multiprocessing
import os
import pickle
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from glob import glob

import geopandas as gpd
import monaco as mc
import numpy as np
import pandas as pd
from lmoments3 import distr
from scipy.stats import gamma
from tqdm.notebook import tqdm


def sa_preprocess(case):
    flow_types = case.constvals['flow_types']
    channel_n = case.invals['Channel Mannings'].val
    overbank_n = case.invals['Overbank Mannings'].val
    adjust_slope = case.invals['Slope Adjustment'].val

    ch_n_opts = case.constvals['channel_n_opts']
    ovb_opts = case.constvals['overbank_n_opts']
    slope_opts = case.constvals['slope_opts']

    ch_n = ch_n_opts[(np.abs(ch_n_opts - channel_n)).argmin()]
    overb_n = ovb_opts[(np.abs(ovb_opts - overbank_n)).argmin()]
    slope_adj = slope_opts[(np.abs(slope_opts - adjust_slope)).argmin()]

    if ch_n > overb_n:
        overb_n = ch_n

    for flow_type in flow_types:
        files = glob(os.path.join(case.constvals['src_path'], f'*{flow_type}_{ch_n}_{overb_n}_{slope_adj}*'))
        if files != []:
            src = pd.read_csv(files[0], compression="gzip")
            src = src.set_index(['branch_id', 'feature_id']).sort_index()
            break
    #     try:
    #     print('la', files[0], type(case.constvals['feature_id']), case.constvals['feature_id'])
    src = src.loc[0, case.constvals['feature_id']]
    src = src[src['HydroID'] == src['HydroID'].iloc[0]]
    #     except:
    #         print(channel_n, overbank_n, adjust_slope, ch_n, overb_n, slope_adj)
    #         raise ValueError("Bad Options")

    flow = case.invals['Streamflow'].val

    #     print(src, ch_n, overb_n, slope_adj, case.constvals['feature_id'])

    # No tuple parens
    return src, channel_n, overbank_n, adjust_slope, flow


def sa_run(src, channel_n, overbank_n, adjust_slope, flow):
    stage = np.interp(flow, src.loc[:, 'discharge_cms'].values, src.loc[:, 'stage'].values)

    df = pd.DataFrame(
        {
            'stage': stage,
            'flow': flow,
            'channel_n': channel_n,
            'overbank_n': overbank_n,
            'adjust_slope': adjust_slope,
        },
        index=[0],
    )

    return df


def sa_postprocess(case, df):

    # Note that for pandas dataframes, you must explicitly include the index
    case.addOutVal('Stage', df['stage'].values[0])
    case.addOutVal('Flow', df['flow'].values[0])
    case.addOutVal('Channel N', df['channel_n'].values[0])
    case.addOutVal('Overbank N', df['overbank_n'].values[0])
    case.addOutVal('Adjust Slope', df['adjust_slope'].values[0])


def sa_monte_carlo_sim(
    ndraws, seed, fcns, feature_id, distr_params, flow_types, src_path, discrete_options, huc08
):
    sim = mc.Sim(
        name=f'flood_inundation_{feature_id}',
        ndraws=ndraws,
        fcns=fcns,
        firstcaseismedian=True,
        samplemethod='sobol_random',
        seed=seed,
        singlethreaded=True,
        #         daskkwargs={'threads_per_worker': 4, 'n_workers': 1},
        savesimdata=False,
        savecasedata=False,
        verbose=True,
        debug=True,
        resultsdir=f"./results_{huc08}",
    )

    sim.addInVar(
        name='Channel Mannings',
        dist=gamma,
        distkwargs={
            'a': distr_params['a'][0],
            'loc': distr_params['loc'][0],
            'scale': distr_params['scale'][0],
        },
    )

    sim.addInVar(
        name='Overbank Mannings',
        dist=gamma,
        distkwargs={
            'a': distr_params['a'][1],
            'loc': distr_params['loc'][1],
            'scale': distr_params['scale'][1],
        },
    )

    sim.addInVar(
        name='Slope Adjustment',
        dist=gamma,
        distkwargs={
            'a': distr_params['a'][2],
            'loc': distr_params['loc'][2],
            'scale': distr_params['scale'][2],
        },
    )

    sim.addInVar(
        name='Streamflow',
        dist=gamma,
        distkwargs={
            'a': distr_params['a'][3],
            'loc': distr_params['loc'][3],
            'scale': distr_params['scale'][3],
        },
    )

    sim.addConstVal(name='channel_n_opts', val=discrete_options['channel_manning_opts'])
    sim.addConstVal(name='overbank_n_opts', val=discrete_options['overbank_manning_opts'])
    sim.addConstVal(name='slope_opts', val=discrete_options['slope_adjustment_opts'])
    sim.addConstVal(name='flow_types', val=flow_types)
    sim.addConstVal(name='feature_id', val=feature_id)
    sim.addConstVal(name='src_path', val=src_path)

    sim.runSim()

    return sim


def sensitivity(huc08, feature_id, src_feats, rec_values, flow_values, lock):
    if feature_id in src_feats and not os.path.exists(
        f"./results_{huc08}/flood_inundation_{feature_id}.mcsim"
    ):

        data = []

        for interval, flow in zip(rec_values, flow_values):

            if interval != "high_water_threshold":
                repeat = int(100 / float('.'.join([x for x in interval.split('_') if x.isnumeric()])))
                data = np.hstack([data, np.repeat(flow, repeat)])

        try:
            flow_parameters = distr.gam.lmom_fit(data)
        except Exception:
            return

        distr_params = {
            'a': [6, 14, 1.5, flow_parameters['a']],
            'loc': [0.02, 0.07, -0.02, flow_parameters['loc']],
            'scale': [0.0076, 0.004, 0.008, flow_parameters['scale']],
        }

        flow_types = ['test', '100yr', 'major', 'moderate', 'minor']

        outputs_dir = '/outputs/'
        src_path = os.path.join(outputs_dir, 'fim_files', 'parameter_output', huc08, 'srcs')

        discrete_options = {
            'channel_manning_opts': np.array([0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1]),
            'overbank_manning_opts': np.array(
                [0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.11, 0.12, 0.13, 0.14, 0.15]
            ),
            'slope_adjustment_opts': np.array([-0.1, -0.05, -0.01, -0.001, 0, 0.001, 0.01, 0.05, 0.1]),
        }

        ndraws = 256
        seed = 12362397
        fcns = {'preprocess': sa_preprocess, 'run': sa_run, 'postprocess': sa_postprocess}

        sim = sa_monte_carlo_sim(
            ndraws, seed, fcns, feature_id, distr_params, flow_types, src_path, discrete_options, huc08
        )

        sim.genCovarianceMatrix()

        try:
            with lock:
                sim.calcSensitivities(outvarnames=['Stage'])

            final_data = {
                "s_indices": sim.outvars['Stage'].sensitivity_indices,
                "s_ratios": sim.outvars['Stage'].sensitivity_ratios,
                "feature_id": feature_id,
                "correlation_coefficients": sim.corrcoeffs,
                "covariance": sim.covs,
                "stage": sim.outvars['Stage'].vals,
                "channel_manning": sim.outvars['Channel N'].vals,
                "overbank_n": sim.outvars['Overbank N'].vals,
                "streamflow": sim.outvars['Flow'].vals,
                "adjust_slope": sim.outvars['Adjust Slope'].vals,
            }

            with open(f"./results_{huc08}/sensitivities_{feature_id}.pkl", "wb") as file:
                pickle.dump(final_data, file)
        except Exception:

            print("Unable to calc sensitivities")

        fig, ax = mc.plot(
            sim.invars['Channel Mannings'], sim.outvars['Stage'], highlight_cases=0, cov_plot=True
        )
        ax.set_title("Randomly Sampled Channel Manning Roughness and Output Response")

        fig.savefig(f"./results_{huc08}/channel_manning_{feature_id}.png")

        fig, ax = mc.plot(
            sim.invars['Overbank Mannings'], sim.outvars['Stage'], highlight_cases=0, cov_plot=True
        )
        ax.set_title("Randomly Sampled Overbank Manning Roughness and Output Response")

        fig.savefig(f"./results_{huc08}/overbank_manning_{feature_id}.png")

        fig, ax = mc.plot(
            sim.invars['Slope Adjustment'], sim.outvars['Stage'], highlight_cases=0, cov_plot=True
        )
        ax.set_title("Randomly Sampled Slope Adjustment and Output Response")

        fig.savefig(f"./results_{huc08}/slope_adjustment_{feature_id}.png")

        fig, ax = mc.plot(sim.invars['Streamflow'], sim.outvars['Stage'], highlight_cases=0, cov_plot=True)
        ax.set_title("Randomly Sampled Streamflow and Output Response")

        fig.savefig(f"./results_{huc08}/streamflow_{feature_id}.png")

        print(time.localtime())


def process_sensitivity_analysis(huc, branches=None):
    if not os.path.exists(f'./results_{huc}'):
        os.makedirs(f'./results_{huc}')

    gdf = gpd.read_file(f'/outputs/fim_outputs/{huc}/nwm_subset_streams.gpkg')
    features = gdf['ID']
    src_df = None

    src_feats = src_df['feature_id'].unique()

    print(time.localtime())
    final_feats = []
    for feat in features:
        if feat in src_feats:
            final_feats.append(feat)

    def feat_generator(huc, final_feats, rec_intervals, rec_values, lock):
        for feat in final_feats:

            flow_values_tmp = rec_intervals.loc[rec_intervals['feature_id'] == feat,]

            if (flow_values_tmp["fit_method"] != "log_fit").values[0]:
                continue

            flow_values = flow_values_tmp.loc[:, rec_values].values[0] * 0.028316831998814504

            data = {
                'huc08': huc,
                'feature_id': feat,
                'rec_values': rec_values,
                'flow_values': flow_values,
                "lock": lock,
            }

            yield data, feat

    m = multiprocessing.Manager()
    lock = m.Lock()
    # f_gen = feat_generator(huc, final_feats, rec_intervals, rec_values, lock)

    # for inp, ids in f_gen:
    #     sensitivity(**inp)

    num_workers = 1
    rec_intervals = None
    rec_values = None

    executor = ProcessPoolExecutor(max_workers=num_workers)
    f_gen = feat_generator(huc, final_feats, rec_intervals, rec_values, lock)
    executor_generator = {executor.submit(sensitivity, **inp): ids for inp, ids in f_gen}
    verbose = False

    for future in tqdm(
        as_completed(executor_generator),
        total=len(executor_generator),
        desc=f"running sensitivities with {num_workers} workers",
        disable=(not verbose),
    ):

        try:
            future.result()
        except Exception:
            print('didnt work!')
