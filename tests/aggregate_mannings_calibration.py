#!/usr/bin/env python3


import os
import pandas as pd

TEMP = r'/data/temp'
huc_list_path = 'data/temp/parameter_adj_files/dev_fim_ble.lst'
branch_name = 'mannings_calibration'

outfolder = os.path.join(TEMP,"mannings_calibration_summary_dev_faf2237_huc8")
aggregate_output_dir = os.path.join(outfolder, 'aggregate_metrics')

if not os.path.exists(aggregate_output_dir):
    os.makedirs(aggregate_output_dir)

outfile = os.path.join(outfolder,'mannings_calibration_summary_dev_faf2237_huc8.csv')

mannings_summary_table = pd.DataFrame(columns = ['metric', 'value', 'stream_order', 'iteration', 'huc', 'interval'])

## temporary mapping
import csv
mapping_file = 'data/temp/parameter_adj_files/iter_mannings_map.csv'
with open(mapping_file, mode='r') as infile:
    iter_mannings_map = {rows[0]:rows[1] for rows in csv.reader(infile)}
##

with open(huc_list_path) as f:
    huc_list = [huc.rstrip() for huc in f]

for huc in huc_list:
    branch_dir = os.path.join('data','test_cases',str(huc) + '_ble','performance_archive','development_versions', branch_name)
    for stream_order in os.listdir(branch_dir):
        stream_order_dir = os.path.join(branch_dir, stream_order)
        for mannings_value in os.listdir(stream_order_dir):
            mannings_value_dir = os.path.join(stream_order_dir, mannings_value)
            for flood_recurrence in os.listdir(mannings_value_dir):
                flood_recurrence_dir = os.path.join(mannings_value_dir, flood_recurrence)
                total_area_stats = pd.read_csv(os.path.join(flood_recurrence_dir, 'total_area_stats.csv'), index_col=0)
                total_area_stats = total_area_stats.loc[['true_positives_count', 'true_negatives_count', 'false_positives_count', 'false_negatives_count','masked_count', 'cell_area_m2', 'CSI'],:]
                total_area_stats = total_area_stats.reset_index()
                total_area_stats_table = pd.DataFrame({'metric': total_area_stats.iloc[:,0], 'value': total_area_stats.iloc[:,1], 'stream_order': stream_order, 'iteration': mannings_value, 'huc': huc, 'interval': flood_recurrence})
                mannings_summary_table = mannings_summary_table.append(total_area_stats_table, ignore_index=True)

mannings_summary_table['mannings_n'] = mannings_summary_table['iteration'].astype(str).map(iter_mannings_map)
mannings_summary_table = mannings_summary_table.drop(columns='iteration')
mannings_summary_table.to_csv(outfile,index=False)

## calculate optimal parameter set
from utils.shared_functions import compute_stats_from_contingency_table

true_positives, true_negatives, false_positives, false_negatives, cell_area, masked_count = 0, 0, 0, 0, 0, 0

list_to_write = [['metric', 'value', 'stream_order', 'mannings_value', 'return_interval']]  # Initialize header.
for stream_order in mannings_summary_table.stream_order.unique():
    for return_interval in mannings_summary_table.interval.unique():
        for mannings_value in mannings_summary_table.mannings_n.unique():
            true_positives = mannings_summary_table.loc[(mannings_summary_table['interval']==return_interval) & (mannings_summary_table['stream_order']==stream_order) & (mannings_summary_table['mannings_n']==mannings_value) & (mannings_summary_table['metric']=='true_positives_count'),'value'].sum()
            true_negatives = mannings_summary_table.loc[(mannings_summary_table['interval']==return_interval) & (mannings_summary_table['stream_order']==stream_order) & (mannings_summary_table['mannings_n']==mannings_value) & (mannings_summary_table['metric']=='true_negatives_count'),'value'].sum()
            false_positives = mannings_summary_table.loc[(mannings_summary_table['interval']==return_interval) & (mannings_summary_table['stream_order']==stream_order) & (mannings_summary_table['mannings_n']==mannings_value) & (mannings_summary_table['metric']=='false_positives_count'),'value'].sum()
            false_negatives = mannings_summary_table.loc[(mannings_summary_table['interval']==return_interval) & (mannings_summary_table['stream_order']==stream_order) & (mannings_summary_table['mannings_n']==mannings_value) & (mannings_summary_table['metric']=='false_negatives_count'),'value'].sum()
            masked_count = mannings_summary_table.loc[(mannings_summary_table['interval']==return_interval) & (mannings_summary_table['stream_order']==stream_order) & (mannings_summary_table['mannings_n']==mannings_value) & (mannings_summary_table['metric']=='masked_count'),'value'].sum()

            cell_area = mannings_summary_table.loc[(mannings_summary_table['interval']==return_interval) & (mannings_summary_table['stream_order']==stream_order) & (mannings_summary_table['mannings_n']==mannings_value) & (mannings_summary_table['metric']=='cell_area_m2'),'value'].sum()

            # Pass all sums to shared function to calculate metrics.
            stats_dict = compute_stats_from_contingency_table(true_negatives, false_negatives, false_positives, true_positives, cell_area=cell_area, masked_count=masked_count)

            for stat in stats_dict:
                list_to_write.append([stat, stats_dict[stat], stream_order, mannings_value, return_interval])

# Map path to output directory for aggregate metrics.
output_file = os.path.join(aggregate_output_dir,'aggregate_metrics_mannings_calibration_by_streamorder.csv')

with open(output_file, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerows(list_to_write)


print("Finished aggregating metrics over " + str(len(huc_list)) + " test cases.")
print("Results are at: " + output_file)
