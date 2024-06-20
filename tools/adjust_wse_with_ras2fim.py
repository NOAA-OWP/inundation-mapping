import os
from multiprocessing import Pool

import geopandas as gpd
import matplotlib.lines as mlines
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from numpy import interp


def get_ras2fim_discharge(args):
    ras_rating_with_stage, this_WSE_base_error, this_point_fid_xs = args
    this_ras_rating = ras_rating_with_stage[ras_rating_with_stage["fid_xs"] == this_point_fid_xs].sort_values(
        by="flow"
    )

    all_flows = this_ras_rating["flow"].values
    all_stage = this_ras_rating["stage"].values

    if this_WSE_base_error > 0:
        adjust_value = interp(this_WSE_base_error, all_stage, all_flows)
    else:
        adjust_value = 0

    return this_point_fid_xs, adjust_value


class adjust_wse_with_ras2fim:
    '''
    This code addresses the issue: https://github.com/NOAA-OWP/inundation-mapping/issues/994
    The results of this work have been used here: https://docs.google.com/presentation/d/1OYqNWQKPgYquWxnJgYcrvuV67iWzwikZ/edit#slide=id.p1
    '''

    def __init__(self, ras_dir, HAND_dir, hand_discharge_type, output_dir):
        self.ras_dir = ras_dir
        self.HAND_dir = HAND_dir
        self.hand_discharge_type = hand_discharge_type
        self.output_dir = output_dir

    def get_hydroIds_QAdjust(self, ras_points_with_rc, ras_rating, HAND_SRC):
        '''
        For the details and the algorithm, see this issue: https://github.com/NOAA-OWP/inundation-mapping/issues/994
        '''

        def get_ras2fim_discharge_parallel(ras_points_with_rc, ras_rating_with_stage):
            args = []
            for i, row in ras_points_with_rc.iterrows():
                args.append((ras_rating_with_stage, row.WSE_base_error, row.fid_xs))

            with Pool(7) as pool:

                list_of_returned_results = pool.map(get_ras2fim_discharge, args)

            ras2fim_Qs = pd.DataFrame(list_of_returned_results, columns=["fid_xs", "Q_Adjust_temp"])
            return ras2fim_Qs

        # the min WSE of ras is needed to compare with HAND min WSE
        min_flow_idx = ras_rating.groupby('fid_xs')['flow'].idxmin()  # use for min flow

        # Use the row indices to extract the corresponding 'WSE' values.
        min_flow_wse = ras_rating.loc[min_flow_idx, ['fid_xs', 'flow', 'wse']]
        min_flow_wse.columns = ['fid_xs', 'ras_min_flow', 'ras_min_wse']

        # merge the results into ras_points_with_rc
        ras_points_with_rc = ras_points_with_rc.merge(min_flow_wse, on='fid_xs')

        # now start to compute min WSE of HAND
        # TODO examine using WSE of actual minimum flow instead of assuming the stage to be zero
        # Note... I tested above and the differenc (improvment in results) ewas insignificant.
        # get DEM values for ras points
        DEM_dataset = rasterio.open(os.path.join(self.HAND_dir, 'dem_thalwegCond_0.tif'))
        ras_points_coords = list(
            zip(ras_points_with_rc.geometry.centroid.x, ras_points_with_rc.geometry.centroid.y)
        )
        ras_points_with_rc["DEM_datum"] = [value[0] for value in DEM_dataset.sample(ras_points_coords)]

        # therefore, HAND minimum WSE is equl to DEM + first stage (this is Ryan method that we call this script after)
        ras_points_with_rc["HAND_min_wse"] = ras_points_with_rc["DEM_datum"].values + 0.3048

        # compute the base error
        ras_points_with_rc["WSE_base_error"] = (
            ras_points_with_rc["HAND_min_wse"] - ras_points_with_rc["ras_min_wse"]
        )

        # convert ras2fim rating WSE to stage
        ras_rating_with_stage = ras_rating.merge(ras_points_with_rc[["fid_xs", "DEM_datum"]], on="fid_xs")
        ras_rating_with_stage["stage"] = ras_rating_with_stage["wse"] - ras_rating_with_stage["DEM_datum"]

        # Interpolate a discharge from ras2fim discharge-stage rating curves for the "WSE_base_error", and call it "Q_Adjust"
        ras2fim_Qs = get_ras2fim_discharge_parallel(ras_points_with_rc, ras_rating_with_stage)
        ras_points_with_rc = ras_points_with_rc.merge(ras2fim_Qs, on="fid_xs")

        # Compute the median of "Q_Adjust" of ras2fim points within each HAND HydroID
        # to do that, first we need to get HydroID for ras points
        HydroID_tif_dataset = rasterio.open(
            os.path.join(self.HAND_dir, "gw_catchments_reaches_filtered_addedAttributes_0.tif")
        )
        ras_points_with_rc["HydroID"] = [value[0] for value in HydroID_tif_dataset.sample(ras_points_coords)]

        # now retrieve the first available Q from HAND SRC
        # as a temp dataset needed, remove stage=0 from HAND SRC so we can apply .min to get first discharge
        # only select HydroIds needed for ras_point
        HAND_SRC = HAND_SRC[HAND_SRC['HydroID'].isin(ras_points_with_rc["HydroID"].unique())]

        HAND_SRC_temp = HAND_SRC[HAND_SRC['stage'] != 0]

        HAND_first_q = HAND_SRC_temp.groupby("HydroID")[self.hand_discharge_type].min().reset_index()
        HAND_first_q.rename(columns={self.hand_discharge_type: "HAND_first_q"}, inplace=True)

        ras_points_with_rc = ras_points_with_rc.merge(HAND_first_q, on='HydroID')

        ras_points_with_rc["Q_Adjust"] = (
            ras_points_with_rc["Q_Adjust_temp"] - ras_points_with_rc["HAND_first_q"]
        )

        # compute median of baseline error Q for the points within each hydroid
        HydroIDs_QAdjust = ras_points_with_rc.groupby("HydroID")["Q_Adjust"].median()
        ras_points_with_rc['Q_HydroId_Median'] = ras_points_with_rc['HydroID'].map(HydroIDs_QAdjust)

        # report the required adusted HAND flows
        return ras_points_with_rc, ras_points_with_rc[['HydroID', 'Q_HydroId_Median']].drop_duplicates(
            subset='HydroID'
        )

    def compare_ras_with_hand(self, ras_points_with_rc, ras_rating, updated_HAND_SRC, flow_intensity):
        '''
        This method compute the difference of WSE between HAND and ras2fim for four different ras2fim flow intensities of q1 (first quantile),
        median, q3 (third quantile) and max.
        The method assumes the existence of updated HAND SRC created in previous step.
        '''

        # find selected method of WSE from ras
        ras_rating["orig_index"] = ras_rating.index.values
        if flow_intensity == 'max':
            method_flow = ras_rating.groupby('fid_xs')['flow'].max().reset_index()
        elif flow_intensity == 'median':
            method_flow = ras_rating.groupby('fid_xs')['flow'].median().reset_index()
        elif flow_intensity == 'q1':
            method_flow = (
                ras_rating.groupby('fid_xs')['flow'].quantile(0.25, interpolation='nearest').reset_index()
            )
        elif flow_intensity == 'q3':
            method_flow = (
                ras_rating.groupby('fid_xs')['flow'].quantile(0.75, interpolation='nearest').reset_index()
            )

        # Merge the flows with the original DataFrame to get the corresponding indices
        merged_df = ras_rating.merge(method_flow, on=['fid_xs', 'flow'], how='inner')

        # Get the indices of the method flow rows
        method_flow_idx = merged_df.set_index('fid_xs')['orig_index']

        # Use the row indices to extract the corresponding 'WSE' values. Also get max flow from here and not "max_flow" of gpkg file
        method_flow_wse = ras_rating.loc[method_flow_idx, ['fid_xs', 'flow', 'wse']]
        method_flow_wse.columns = ['fid_xs', 'ras_%s_flow' % flow_intensity, 'ras_%s_wse' % flow_intensity]

        # merge the results into ras_points_with_rc
        ras_points_with_rc = ras_points_with_rc.merge(method_flow_wse, on='fid_xs')

        # now get results at points  using HAND
        adjusted_HAND_stage = []
        original_HAND_stage = []
        for i, row in ras_points_with_rc.iterrows():
            this_point_method_flow, this_point_HydroID = row["ras_%s_flow" % flow_intensity], row.HydroID
            this_SRC = updated_HAND_SRC[updated_HAND_SRC["HydroID"] == this_point_HydroID].sort_values(
                by=self.hand_discharge_type
            )

            all_flows_adjust = this_SRC[self.hand_discharge_type].values
            all_flows_orig = this_SRC['original_' + self.hand_discharge_type].values
            all_stages = this_SRC["stage"].values

            # Perform linear interpolation
            adjusted_HAND_stage.append(interp(this_point_method_flow, all_flows_adjust, all_stages))
            original_HAND_stage.append(interp(this_point_method_flow, all_flows_orig, all_stages))

        ras_points_with_rc["Adjusted_HAND_%s_stage" % flow_intensity] = np.array(adjusted_HAND_stage)
        ras_points_with_rc["Original_HAND_%s_stage" % flow_intensity] = np.array(original_HAND_stage)

        ras_points_with_rc["Adjusted_HAND_%s_wse" % flow_intensity] = (
            ras_points_with_rc["Adjusted_HAND_%s_stage" % flow_intensity] + ras_points_with_rc["DEM_datum"]
        )
        ras_points_with_rc["Original_HAND_%s_wse" % flow_intensity] = (
            ras_points_with_rc["Original_HAND_%s_stage" % flow_intensity] + ras_points_with_rc["DEM_datum"]
        )

        ras_points_with_rc["Adjusted_HAND_%s_wse" % flow_intensity] = np.where(
            ras_points_with_rc["Original_HAND_%s_wse" % flow_intensity].values
            > ras_points_with_rc['ras_%s_wse' % flow_intensity].values,
            ras_points_with_rc["Adjusted_HAND_%s_wse" % flow_intensity].values,
            ras_points_with_rc["Original_HAND_%s_wse" % flow_intensity].values,
        )

        ras_points_with_rc["Orig_Error"] = (
            ras_points_with_rc["ras_%s_wse" % flow_intensity]
            - ras_points_with_rc["Original_HAND_%s_wse" % flow_intensity]
        )
        ras_points_with_rc["Adj_Error"] = (
            ras_points_with_rc["ras_%s_wse" % flow_intensity]
            - ras_points_with_rc["Adjusted_HAND_%s_wse" % flow_intensity]
        )

        ras_points_with_rc.drop(
            columns=["Adjusted_HAND_%s_stage" % flow_intensity, "Original_HAND_%s_stage" % flow_intensity],
            inplace=True,
        )

        return ras_points_with_rc

    def comparison_plots(self, ras_points_Adjusted_Q, flow_intensity):
        '''
        This method make difference plots using both original HAND SRC and updated HAND SRC.
        '''
        if not os.path.exists(os.path.join(self.output_dir, 'plots')):
            os.makedirs(os.path.join(self.output_dir, 'plots'))

        for error_type in ['Orig', 'Adj']:
            ras_points_Adjusted_Q["residual_color"] = np.where(
                ras_points_Adjusted_Q["%s_Error" % error_type].values > 0, "red", "blue"
            )

            RMSE = np.sqrt(np.mean(ras_points_Adjusted_Q["%s_Error" % error_type].values ** 2))
            MAE = np.mean(abs(ras_points_Adjusted_Q["%s_Error" % error_type].values))
            ras_points_Adjusted_Q.plot(color=ras_points_Adjusted_Q["residual_color"].values, markersize=4)

            # Create custom legend markers (two points)
            legend_marker2 = mlines.Line2D(
                [], [], color='blue', marker='o', linestyle='', markersize=4, label='HAND > RAS'
            )
            legend_marker1 = mlines.Line2D(
                [], [], color='red', marker='o', linestyle='', markersize=4, label='RAS > HAND'
            )

            # Add the custom legend markers to the legend
            plt.legend(handles=[legend_marker1, legend_marker2])
            plt.title(
                "WSE comparison between RAS2FIM and %s HAND for %s flows\nRMSE=%.2lfm, MAE=%.2lfm"
                % (error_type, flow_intensity, RMSE, MAE)
            )
            plt.axis('off')

            plt.savefig(
                os.path.join(self.output_dir, 'plots', 'plot1_%s_%s.png' % (error_type, flow_intensity))
            )
            plt.close()

            plt.axhline(y=0, color="blue", linestyle='--')
            plt.scatter(
                ras_points_Adjusted_Q["ras_%s_wse" % flow_intensity].values,
                ras_points_Adjusted_Q["%s_Error" % error_type].values,
                color="red",
                s=4,
            )

            plt.xlabel("RAS2FIM WSE (m)")
            plt.ylabel("RAS2FIM WSE minus HAND WSE (m)")
            plt.title("For RAS2FIM %s flows using %s HAND SRC" % (flow_intensity, error_type))
            plt.savefig(
                os.path.join(self.output_dir, 'plots', 'plot2_%s_%s.png' % (error_type, flow_intensity))
            )
            plt.close()


if __name__ == '__main__':
    # specify required inputs
    ras_dir = r"../../output_ras2fim_12090301"
    HAND_dir = r"../../Ali_12090301\12090301\branches\0"
    output_dir = r"../results/final"
    hand_discharge_type = 'precalb_discharge_cms'  # options are 'discharge_cms' or 'precalb_discharge_cms'
    obs = adjust_wse_with_ras2fim(ras_dir, HAND_dir, hand_discharge_type, output_dir)

    # read inputs
    print('Reading input files')
    ras_points = gpd.read_file(os.path.join(ras_dir, "reformat_ras_rating_curve_points.gpkg"))
    ras_points = ras_points[['fid_xs', 'feature_id', 'geometry']]

    # read rating curve info
    ras_rating = pd.read_csv(
        os.path.join(ras_dir, 'reformat_ras_rating_curve_table-12090301.csv'),
        usecols=['fid_xs', 'wse', 'flow'],
    )

    # remove the points not having info in rating curve file
    ras_points_with_rc = ras_points[ras_points["fid_xs"].isin(ras_rating["fid_xs"].unique())]

    HAND_SRC = pd.read_csv(
        os.path.join(HAND_dir, 'hydroTable_0.csv'), usecols=['HydroID', 'stage', hand_discharge_type]
    )

    # first get the hydroIds Q adjust
    print('Calculating HAND HydroIDs Q adjust')
    ras_points_Adjusted_Q, HydroIDs_Adjusted_Q = obs.get_hydroIds_QAdjust(
        ras_points_with_rc, ras_rating, HAND_SRC
    )
    ras_points_Adjusted_Q.to_file(os.path.join(output_dir, "ras_points_Adjusted_Q.gpkg"))
    HydroIDs_Adjusted_Q.to_csv(os.path.join(output_dir, "HydroIDs_Adjusted_Q.csv"), index=False)

    # step 2...Adjust HAND SRCs
    print('Updating HAND SRC with Q adjust values')
    # TODO see why we have negative Q ajust for some hydroIDs...for now make them zero
    HydroIDs_Adjusted_Q = HydroIDs_Adjusted_Q[HydroIDs_Adjusted_Q['Q_HydroId_Median'] > 0]

    Orig_HAND_SRC = HAND_SRC.copy()
    Orig_HAND_SRC.rename(columns={hand_discharge_type: 'original_%s' % hand_discharge_type}, inplace=True)

    updated_HAND_SRC = Orig_HAND_SRC.merge(HydroIDs_Adjusted_Q, on='HydroID', how='left')

    # assign zero for the hydroids that do not have any ras points
    updated_HAND_SRC['Q_HydroId_Median'].fillna(0, inplace=True)

    updated_HAND_SRC[hand_discharge_type] = (
        updated_HAND_SRC['original_%s' % hand_discharge_type] + updated_HAND_SRC['Q_HydroId_Median']
    )
    updated_HAND_SRC.to_csv(os.path.join(output_dir, 'hydroTable_0_updated.csv'), index=False)

    # finally compare RAS with HAND... this prepare difference between HAND and RAS2fim for both original HAND SRC and updated HAND SRC.
    print('Evaluating the difference between HAND and SRC for different flow intensities:')
    ras_points_Adjusted_Q = ras_points_Adjusted_Q[
        ['fid_xs', 'feature_id', 'DEM_datum', 'HydroID', 'Q_HydroId_Median', 'geometry']
    ]

    MAE_summary = []
    for flow_intensity in ['q1', 'median', 'q3', 'max']:
        print('working on %s' % flow_intensity)
        ras_points_Adjusted_Q = obs.compare_ras_with_hand(
            ras_points_Adjusted_Q, ras_rating, updated_HAND_SRC, flow_intensity
        )
        ras_points_Adjusted_Q.to_file(os.path.join(output_dir, "RAS_HAND_Diff_%s.gpkg" % flow_intensity))
        obs.comparison_plots(ras_points_Adjusted_Q, flow_intensity)

        # also record MAE
        MAE_orig_error = np.mean(abs(ras_points_Adjusted_Q['Orig_Error'].values))
        MAE_Adj_error = np.mean(abs(ras_points_Adjusted_Q['Adj_Error'].values))

        MAE_improve = 100 * (MAE_orig_error - MAE_Adj_error) / MAE_orig_error
        MAE_summary.append([flow_intensity, MAE_orig_error, MAE_Adj_error, MAE_improve])
    MAE_summary_DF = pd.DataFrame(
        MAE_summary,
        columns=["flow_intensity", 'MAE_difference_original', 'MAE_difference_updated', 'improvment%'],
    )
    MAE_summary_DF.to_csv(os.path.join(output_dir, 'MAE_stats.csv'), index=False)
