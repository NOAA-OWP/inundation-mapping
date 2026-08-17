import argparse
import sys

import geopandas as gpd

from src.utils.spatial import sjoin


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


def trace():
    import inspect
    import traceback

    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    line = tbinfo.split(", ")[1]
    filename = inspect.getfile(inspect.currentframe())
    # Get Python syntax error
    synerror = traceback.format_exc().splitlines()[-1]
    return line, filename, synerror


from_node = "From_Node"
to_node = "To_Node"
next_down_id = "NextDownID"


class build_stream_traversal_columns(object):
    '''Tool class for updating the next down IDs of stream features.'''

    def __init__(self):
        '''Define tool properties (tool name is the class name).'''
        self.label = 'Find Next Downstream Line'
        self.description = (
            '''Finds next downstream line, retrieves its HydroID and stores it in the NextDownID field.'''
        )

    def execute(self, streams, wbd8, hydro_id):
        try:
            sOK = 'OK'

            # 1. Check for HydroID; Assign if missing
            if hydro_id not in streams.columns:
                print("Required field " + hydro_id + " does not exist in input. Generating..")

                # Get stream midpoints safely
                stream_midpoint = [line.interpolate(0.5, normalized=True) for line in streams.geometry]
                stream_md_gpd = gpd.GeoDataFrame(
                    {'geometry': stream_midpoint}, crs=streams.crs, geometry='geometry'
                )

                # Execute spatial join
                stream_wbdjoin = sjoin(stream_md_gpd, wbd8, how='left', predicate='within')

                # Extract HUC ID from spatial join result safely
                huc_col = next(
                    (c for c in stream_wbdjoin.columns if c.lower() in ['fimid', 'fim_id', 'huc8']), None
                )
                if huc_col:
                    streams['HUC8id'] = stream_wbdjoin[huc_col].values
                elif 'index_right' in stream_wbdjoin.columns:
                    streams['HUC8id'] = stream_wbdjoin['index_right'].values
                else:
                    streams['HUC8id'] = "00000000"

                streams['seqID'] = (
                    (streams.groupby('HUC8id', dropna=False).cumcount(ascending=True) + 1)
                    .astype('str')
                    .str.zfill(4)
                )
                streams = streams.loc[streams['HUC8id'].notna(), :]
                streams['HUC8id'] = streams['HUC8id'].astype(str)
                streams['seqID'] = streams['seqID'].astype(str)

                streams = streams.assign(hydro_id=lambda x: x.HUC8id + x.seqID)
                streams = streams.rename(columns={"hydro_id": hydro_id}).sort_values(hydro_id)
                streams = streams.drop(columns=['HUC8id', 'seqID'])
                streams[hydro_id] = streams[hydro_id].astype(int)
                print('Generated ' + hydro_id)

            # 2. Normalize existing column names if sjoin appended suffixes (_left/_right)
            for col in [from_node, to_node]:
                if col not in streams.columns:
                    if f"{col}_left" in streams.columns:
                        streams.rename(columns={f"{col}_left": col}, inplace=True)
                    elif f"{col}_right" in streams.columns:
                        streams.rename(columns={f"{col}_right": col}, inplace=True)

            # 3. Generate To/From Nodes if missing or empty
            bOK = True
            if from_node not in streams.columns or streams[from_node].eq('').all():
                print("Field " + from_node + " does not exist or is empty in input. Generating..")
                bOK = False
            if to_node not in streams.columns or streams[to_node].eq('').all():
                print("Field " + to_node + " does not exist or is empty in input. Generating..")
                bOK = False

            if not bOK:
                streams[from_node] = ''
                streams[to_node] = ''

                streams = streams.sort_values(by=[hydro_id], ascending=True).copy()

                xy_dict = {}
                bhasnullshape = False
                for idx, row in streams[['geometry', from_node, to_node]].iterrows():
                    geom = row['geometry']
                    if geom and not geom.is_empty:
                        # From Node
                        firstx = round(geom.coords.xy[0][0], 7)
                        firsty = round(geom.coords.xy[1][0], 7)
                        from_key = f"{firstx},{firsty}"
                        if from_key not in xy_dict:
                            xy_dict[from_key] = len(xy_dict) + 1
                        streams.at[idx, from_node] = xy_dict[from_key]

                        # To Node
                        lastx = round(geom.coords.xy[0][-1], 7)
                        lasty = round(geom.coords.xy[1][-1], 7)
                        to_key = f"{lastx},{lasty}"
                        if to_key not in xy_dict:
                            xy_dict[to_key] = len(xy_dict) + 1
                        streams.at[idx, to_node] = xy_dict[to_key]
                    else:
                        bhasnullshape = True

                if bhasnullshape:
                    print("Some of the input features have a null shape.")
                else:
                    print('Generated To/From Nodes')

            # 4. Create NextDownID field
            if next_down_id not in streams.columns:
                streams[next_down_id] = -1

            # Build downstream node lookup dictionary
            dnodes = {}
            for _, row in streams[[from_node, hydro_id]].iterrows():
                fn = row[from_node]
                hid = row[hydro_id]
                if fn in dnodes:
                    dnodes[fn].append(hid)
                else:
                    dnodes[fn] = [hid]

            # Populate NextDownID
            for idx, urow in streams[[next_down_id, to_node, hydro_id]].iterrows():
                tn = urow[to_node]
                next_down_ids = dnodes.get(tn, [])

                if len(next_down_ids) == 0:
                    nextdownIDcol = -1
                elif len(next_down_ids) == 1:
                    nextdownIDcol = next_down_ids[0]
                else:
                    nextdownIDcol = next_down_ids[0]

                streams.at[idx, next_down_id] = nextdownIDcol

            # Guarantee From_Node and To_Node exist cleanly on output
            streams[from_node] = streams[from_node].astype(str)
            streams[to_node] = streams[to_node].astype(str)

            tReturns = (sOK, streams)

        except Exception:
            sOK = "{}".format(trace())
            tReturns = (sOK,)
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
