'''
Description:
        This tool creates unique IDs for each segment and builds the To_Node, From_Node, and NextDownID columns to traverse the network
Required Arguments:
        streams   = stream network
        wbd8          = HUC8 boundary dataset
        hydro_id       = name of ID column (string)
'''
import sys
import pandas as pd
import argparse
import geopandas as gpd

def trace():
    import traceback, inspect
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
        self.description = '''Finds next downstream line, retrieves its HydroID and stores it in the NextDownID field.'''

    def execute(self, streams, wbd8, hydro_id):
        try:
            split_code = 1
            sOK = 'OK'

            # check for HydroID; Assign if it doesn't exist
            if not hydro_id in streams.columns:
                print ("Required field " + hydro_id + " does not exist in input. Generating..")

                # Get stream midpoint
                stream_midpoint = []
                for i,lineString in enumerate(streams.geometry):
                    stream_midpoint = stream_midpoint + [lineString.interpolate(0.5,normalized=True)]

                stream_md_gpd = gpd.GeoDataFrame({'geometry':stream_midpoint}, crs=streams.crs, geometry='geometry')
                stream_wbdjoin = gpd.sjoin(stream_md_gpd, wbd8, how='left', op='within')
                stream_wbdjoin = stream_wbdjoin.rename(columns={"geometry": "midpoint", "index_right": "HUC8id"})
                streams = streams.join(stream_wbdjoin).drop(columns=['midpoint'])

                streams['seqID'] = (streams.groupby('HUC8id').cumcount(ascending=True)+1).astype('str').str.zfill(4)
                streams = streams.loc[streams['HUC8id'].notna(),:]
                if streams.HUC8id.dtype != 'str': streams.HUC8id = streams.HUC8id.astype(str)
                if streams.seqID.dtype != 'str': streams.seqID = streams.seqID.astype(str)

                streams = streams.assign(hydro_id= lambda x: x.HUC8id + x.seqID)
                streams = streams.rename(columns={"hydro_id": hydro_id}).sort_values(hydro_id)
                streams = streams.drop(columns=['HUC8id', 'seqID'])
                streams[hydro_id] = streams[hydro_id].astype(int)
                print ('Generated ' + hydro_id)

            # Check for TO/From Nodes; Assign if doesnt exist
            bOK = True
            if not from_node in streams.columns:
                print ("Field " + from_node + " does not exist in input ")
                bOK = False
            if not to_node in streams.columns:
                print ("Field " + to_node + " does not exist in input. Generating..")
                bOK = False

            if(bOK==False):
                # Add fields if not they do not exist.
                if not from_node in streams.columns:
                    streams[from_node] = ''

                if not to_node in streams.columns:
                    streams[to_node] = ''

                streams = streams.sort_values(by=[hydro_id], ascending=True).copy()

                xy_dict = {}
                bhasnullshape=False
                for rows in streams[['geometry', from_node, to_node]].iterrows():
                    if rows[1][0]:
                        # From Node
                        firstx = round(rows[1][0].coords.xy[0][0], 7)
                        firsty = round(rows[1][0].coords.xy[1][0], 7)
                        from_key = '{},{}'.format(firstx, firsty)
                        if from_key in xy_dict:
                            streams.at[rows[0], from_node,] = xy_dict[from_key]
                        else:
                            xy_dict[from_key] = len(xy_dict) + 1
                            streams.at[rows[0], from_node,] = xy_dict[from_key]

                        # To Node
                        lastx = round(rows[1][0].coords.xy[0][-1], 7)
                        lasty = round(rows[1][0].coords.xy[1][-1], 7)
                        to_key = '{},{}'.format(lastx, lasty)
                        #if xy_dict.has_key(to_key):
                        if to_key in xy_dict:
                            streams.at[rows[0], to_node] = xy_dict[to_key]
                        else:
                            xy_dict[to_key] = len(xy_dict) + 1
                            streams.at[rows[0], to_node] = xy_dict[to_key]
                    else:
                         bhasnullshape=True

                if bhasnullshape==True:
                    print ("Some of the input features have a null shape.")
                    print (from_node + " and " + to_node + " fields cannot be populated for those features.")
                else:
                    print ('Generated To/From Nodes')

            # Create NextDownID field
            if not next_down_id in streams.columns:
                streams[next_down_id] = ''

            # Create dict to store from_node values for each HydroID
            dnodes=dict()
            lstHydroIDs=[]
            for row in streams[[from_node,hydro_id]].iterrows():

                if (row[1][0] in dnodes)==False:
                    lstHydroIDs=[row[1][1]]
                    dnodes.setdefault(row[1][0],lstHydroIDs)
                else:
                    lstHydroIDs = dnodes[row[1][0]]
                    lstHydroIDs.append(row[1][1])

            # for each stream segment, search dict for HydroID downstream and
            for urow in streams[[next_down_id, to_node, from_node, hydro_id]].iterrows():
                tonodecol = urow[1][1]
                nextdownIDcol = urow[1][0]
                hydroIDcol = urow[1][3]
                try:
                    next_down_ids = dnodes[tonodecol]
                except:
                    next_down_ids=[]
                splits = len(next_down_ids)
                if splits == 0: # terminal segment
                    nextdownIDcol = -1
                elif splits == 1:
                    nextdownIDcol = next_down_ids[0]
                else: # either muliple inflows or
                    i = 0
                    for nid in next_down_ids:
                        # Set the split code in the NextDownID field.
                        if split_code < 0: # set terminal NextDownID field to -1; this should never happen..
                            if i == 0:
                                nextdownIDcol = split_code
                        else: # muliple inflows
                            if i == 0:
                                nextdownIDcol = nid
                        i += 1
                try:
                    if next_down_ids:del next_down_ids
                except:
                    pass
                streams.loc[streams[hydro_id]== hydroIDcol,[next_down_id]] = nextdownIDcol

            tReturns = (sOK, streams)
        except Exception:
            sOK = "{}".format(trace())
            tReturns = (sOK, )
        return tReturns

if(__name__=='__main__'):
    try:
        ap = argparse.ArgumentParser()
        ap.add_argument("-p", "--parameters", nargs='+', default=[], required=True,
                    help="list of parameters")
        args = ap.parse_args()
        streams    = args.parameters[0]
        wbd8           = args.parameters[1]
        hydro_id        = args.parameters[2]

        oProcessor = build_stream_traversal_columns()
        params = (streams, wbd8, hydro_id)
        tResults=None
        tResults = oProcessor.execute(params)

        del oProcessor
    except:
        print (str(trace()))
