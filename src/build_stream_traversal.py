import argparse
import sys

import geopandas as gpd
import numpy as np
import pandas as pd


gpd.options.io_engine = "pyogrio"


'''
Description:
        This tool creates unique IDs for each segment and builds the To_Node, From_Node, and NextDownID
        columns to traverse the network.
Required Arguments:
        streams   = stream network
        wbd8          = HUC8 boundary dataset
        hydro_id       = name of ID column (string)
'''
class build_stream_traversal_columns(object):
    '''Tool class for updating the next down IDs of stream features.'''

    def __init__(self):
        '''Define tool properties (tool name is the class name).'''
        self.label = 'Find Next Downstream Line'
        self.description = (
            '''Finds next downstream line, retrieves its HydroID and stores it in the NextDownID field.'''
        )

    def execute(self, streams_gdf, wbd8, hydro_id):
        split_code = 1
        sOK = 'OK'
        from_node = "From_Node"
        to_node = "To_Node"
        next_down_id = "NextDownID"

        # check for HydroID; Assign if it doesn't exist
        if hydro_id not in streams_gdf.columns:
            print("Required field " + hydro_id + " does not exist in input. Generating..")

            # Get stream midpoint
            stream_midpoint = []
            for i, lineString in enumerate(streams_gdf.geometry):
                stream_midpoint = stream_midpoint + [lineString.interpolate(0.5, normalized=True)]

            stream_md_gpd = gpd.GeoDataFrame(
                {'geometry': stream_midpoint}, crs=streams_gdf.crs, geometry='geometry'
            )
            stream_wbdjoin = gpd.sjoin(stream_md_gpd, wbd8, how='left', predicate='within')
            stream_wbdjoin = stream_wbdjoin.rename(columns={"geometry": "midpoint", "fimid": "HUC8id"})
            streams_gdf = streams_gdf.join(stream_wbdjoin).drop(columns=['midpoint'])

            streams_gdf['seqID'] = (
                (streams_gdf.groupby('HUC8id', dropna=False).cumcount(ascending=True) + 1)
                .astype('str')
                .str.zfill(4)
            )
            streams_gdf = streams_gdf.loc[streams_gdf['HUC8id'].notna(), :]
            if streams_gdf.HUC8id.dtype != 'str':
                streams_gdf.HUC8id = streams_gdf.HUC8id.astype(str)
            if streams_gdf.seqID.dtype != 'str':
                streams_gdf.seqID = streams_gdf.seqID.astype(str)

            streams_gdf = streams_gdf.assign(hydro_id=lambda x: x.HUC8id + x.seqID)
            streams_gdf = streams_gdf.rename(columns={"hydro_id": hydro_id}).sort_values(hydro_id)
            streams_gdf = streams_gdf.drop(columns=['HUC8id', 'seqID'])
            streams_gdf[hydro_id] = streams_gdf[hydro_id].astype(int)
            print('Generated ' + hydro_id)

        # Check for TO/From Nodes; Assign if doesnt exist
        bOK = True
        if from_node not in streams_gdf.columns:
            print("Field " + from_node + " does not exist in input ")
            bOK = False
        if to_node not in streams_gdf.columns:
            print("Field " + to_node + " does not exist in input. Generating..")
            bOK = False

        if bOK is False:
            # Add fields if not they do not exist.
            if from_node not in streams_gdf.columns:
                streams_gdf[from_node] = np.int32(-1)
            else:
                streams_gdf[from_node] = streams_gdf[from_node].astype('Int32')

            if to_node not in streams_gdf.columns:
                streams_gdf[to_node] =  np.int32(-1)
            else:
                streams_gdf[to_node] = streams_gdf[to_node].astype('Int32')

            streams_gdf = streams_gdf.sort_values(by=[hydro_id], ascending=True).copy()
    
            xy_dict = {}
            for row in streams_gdf.itertuples():
                if pd.isna(row.geometry):  # Hummm.. would any ever have missing geoms?
                    print(f"Possible Error: hydro_id: {hydro_id} has a record with a missing geometry." \
                            " Research required.")
                else:
                    # print("++++++")
                    # print(f"idx is {row.Index}")

                    # if row.Index > 2:
                    #     print("all done")
                    #     sys.exit(1)

                    firstx = round(row.geometry.coords.xy[0][0], 7)
                    firsty = round(row.geometry.coords.xy[1][0], 7)
                    from_key = '{},{}'.format(firstx, firsty)
                    if from_key in xy_dict:
                        # print(f"from key is {xy_dict[from_key]}")
                        streams_gdf.at[row.Index, from_node] = xy_dict[from_key]
                    else:
                        #print(f"len(xy_dict) + 1 is {len(xy_dict) + 1}")
                        xy_dict[from_key] = len(xy_dict) + 1
                        streams_gdf.at[row.Index, from_node] = xy_dict[from_key]

                    # print("xy_dict is ...")
                    # print(xy_dict)

                    # To Node
                    lastx = round(row.geometry.coords.xy[0][-1], 7)
                    lasty = round(row.geometry.coords.xy[1][-1], 7)
                    to_key = '{},{}'.format(lastx, lasty)
                    # if xy_dict.has_key(to_key):
                    if to_key in xy_dict:
                        streams_gdf.at[row.Index, to_node] = xy_dict[to_key]
                    else:
                        xy_dict[to_key] = len(xy_dict) + 1
                        streams_gdf.at[row.Index, to_node] = xy_dict[to_key]

            if streams_gdf['geometry'].isna().any():
                print("Some of the input features have a null shape.")
                print(from_node + " and " + to_node + " fields cannot be populated for those features.")
            else:
                print('Generated To/From Nodes')

        # Create NextDownID field
        if next_down_id not in streams_gdf.columns:
            streams_gdf[next_down_id] = np.int32(-1)

        # Create dict to store from_node values for each HydroID
        dnodes = dict()
        lstHydroIDs = []  # TODO: Oct 2026: This variable does not make sense. Why do we even need it?
        # for row in streams[[from_node, hydro_id]].iterrows():
        # Oct 2026: For GeoDataframes (not Dataframes), itertuples is much faster.
        for row in streams_gdf.itertuples():
            # print("++++++++++++++")
            # print(f"idx is {row.Index}")

            # if row.Index > 2:
            #     print("all done")
            #     sys.exit(1)                

            # print(f"row.From_Node is {row.From_Node}")

            # if (row[1][from_node] in dnodes) is False:
            if row.From_Node not in dnodes:
                # lstHydroIDs = [row.HydroID]
                # print("here 1")
                # print("........... lstHydroIDs is")                    
                # print(lstHydroIDs)
                # TODO: Oct 2026: Does the second element really does not need to be a list?
                dnodes.setdefault(row.From_Node, [row.HydroID])
            else:
                # TODO: Oct 2026: This has to be wrong, why keep writing to itself?
                lstHydroIDs = dnodes[row.From_Node]
                # print("here 2")
                # print("........... lstHydroIDs is")                    
                # print(lstHydroIDs)
                lstHydroIDs.append(lstHydroIDs)

        # print("............ dnodes is ")                
        # print(dnodes)
        # print("................") 

        # for each stream segment, search dict for HydroID downstream and
        # for urow in streams[[next_down_id, to_node, from_node, hydro_id]].iterrows():
        for row in streams_gdf.itertuples():
            # print("++++++++++++++")
            # print(f"idx is {row.Index}")

            # if row.Index > 2:
            #     print("all done")
            #     sys.exit(1)       

            tonodecol = row.To_Node
            nextdownIDcol = row.NextDownID
            hydroIDcol = row.HydroID
            # print(f"... row is {row.NextDownID}; {row.To_Node}; {row.From_Node}; {row.HydroID}")                
            try:
                next_down_ids = dnodes[tonodecol]
            except:
                next_down_ids = []
            # print(f"next_down_ids is now {next_down_ids}")
            splits = len(next_down_ids)
            if splits == 0:  # terminal segment
                nextdownIDcol = -1
            elif splits == 1:
                nextdownIDcol = next_down_ids[0]
            else:  # either muliple inflows or
                i = 0
                for nid in next_down_ids:
                    # Set the split code in the NextDownID field.
                    if split_code < 0:  # set terminal NextDownID field to -1; this should never happen.
                        if i == 0:
                            nextdownIDcol = split_code
                    else:  # muliple inflows
                        if i == 0:
                            nextdownIDcol = nid
                    i += 1
            try:
                if next_down_ids:
                    del next_down_ids
            except:
                pass

            streams_gdf.loc[streams_gdf[hydro_id] == hydroIDcol, [next_down_id]] = nextdownIDcol

        # streams = streams.sort_values(by=hydro_id).reset_index(drop=True)

        tReturns = (sOK, streams_gdf)

        # except Exception as ex:
        #     raise ex
        #     # sOK = "{}".format(trace())
        #     # tReturns = (sOK,)
        return tReturns


if __name__ == '__main__':
    try:
        ap = argparse.ArgumentParser()
        ap.add_argument("-p", "--parameters", nargs='+', default=[], required=True, help="list of parameters")
        args = ap.parse_args()
        streams = args.parameters[0]
        wbd8 = args.parameters[1]
        hydro_id = args.parameters[2]

        oProcessor = build_stream_traversal_columns()
        params = (streams, wbd8, hydro_id)
        tResults = None
        tResults = oProcessor.execute(params)

        del oProcessor
    except Exception as e:
        print(repr(e))
