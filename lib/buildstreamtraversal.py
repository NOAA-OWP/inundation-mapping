'''
@author: brian.avant
Description: 
        This tool creates unique IDs for each segment and build the To_Node, From_Node, and NExtDownID columns to traverse the network
Required Arguments:
        modelstream        = stream network
'''
import sys
import datetime
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

FN_FROMNODE = "From_Node"
FN_TONODE = "To_Node"
FN_NEXTDOWNID = "NextDownID"

class BuildStreamTraversalColumns(object):
    '''Tool class for updating the next down IDs of stream features.'''
    def __init__(self):
        '''Define tool properties (tool name is the class name).'''
        self.label = 'Find Next Downstream Line'
        self.description = '''Finds next downstream line, retrieves its HydroID and stores it in the NextDownID field.'''

    def execute(self, modelstream, WBD8, HYDROID):
        try:    
            split_code = 1
            sOK = 'OK' 
                                   
            # check for HydroID; Assign if it doesn't exist
            if not HYDROID in modelstream.columns:
                print ("Required field " + HYDROID + " does not exist in input. Generating..")
                stream_centroid = gpd.GeoDataFrame({'geometry':modelstream.geometry.centroid}, crs=modelstream.crs, geometry='geometry')
                stream_wbdjoin = gpd.sjoin(stream_centroid, WBD8, how='left', op='within')
                stream_wbdjoin = stream_wbdjoin.rename(columns={"geometry": "centroid", "index_right": "HUC8id"})
                modelstream = modelstream.join(stream_wbdjoin).drop(columns=['centroid'])

                modelstream['seqID'] = (modelstream.groupby('HUC8id').cumcount(ascending=True)+1).astype('str').str.zfill(4)
                modelstream = modelstream.loc[modelstream['HUC8id'].notna(),:]
                modelstream = modelstream.assign(HYDROID= lambda x: x.HUC8id + x.seqID)
                modelstream = modelstream.rename(columns={"HYDROID": HYDROID}).sort_values(HYDROID)
                modelstream = modelstream.drop(columns=['HUC8id', 'seqID'])
                modelstream[HYDROID] = modelstream[HYDROID].astype(int)
                print ('Generated ' + HYDROID + ' Successfully')
            
            # Check for TO/From Nodes; Assign if doesnt exist
            bOK = True           
            if not FN_FROMNODE in modelstream.columns:
                print ("Field " + FN_FROMNODE + " does not exist in input ")
                bOK = False  
            if not FN_TONODE in modelstream.columns:
                print ("Field " + FN_TONODE + " does not exist in input. Generating..")
                bOK = False 

            if(bOK==False): 
                # Add fields if not they do not exist.
                if not FN_FROMNODE in modelstream.columns:
                    modelstream[FN_FROMNODE] = ''
                
                if not FN_TONODE in modelstream.columns:
                    modelstream[FN_TONODE] = ''
                
                # PU_Order = 'PU_Order'
                # # Create PU_Order field
                # if not PU_Order in modelstream.columns:
                #     modelstream[PU_Order] = ''
                
                modelstream = modelstream.sort_values(by=[HYDROID], ascending=True).copy()
                
                xy_dict = {}
                bhasnullshape=False
                for rows in modelstream[['geometry', FN_FROMNODE, FN_TONODE]].iterrows():             
                    if rows[1][0]:
                        # From Node
                        firstx = round(rows[1][0].coords.xy[0][0], 7)
                        firsty = round(rows[1][0].coords.xy[1][0], 7)
                        from_key = '{},{}'.format(firstx, firsty)
                        if from_key in xy_dict:
                            modelstream.at[rows[0], FN_FROMNODE,] = xy_dict[from_key]
                        else:
                            xy_dict[from_key] = len(xy_dict) + 1
                            modelstream.at[rows[0], FN_FROMNODE,] = xy_dict[from_key]

                        # To Node
                        lastx = round(rows[1][0].coords.xy[0][-1], 7)
                        lasty = round(rows[1][0].coords.xy[1][-1], 7)
                        to_key = '{},{}'.format(lastx, lasty)
                        #if xy_dict.has_key(to_key):
                        if to_key in xy_dict:
                            modelstream.at[rows[0], FN_TONODE] = xy_dict[to_key]
                        else:
                            xy_dict[to_key] = len(xy_dict) + 1
                            modelstream.at[rows[0], FN_TONODE] = xy_dict[to_key]
                    else:
                         bhasnullshape=True

                if bhasnullshape==True:
                    print ("Some of the input features have a null shape.")
                    print (FN_FROMNODE + " and " + FN_TONODE + " fields cannot be populated for those features.")
                else:          
                    print ('Generated To/From Nodes Successfully')
                
            # Create NextDownID field
            if not FN_NEXTDOWNID in modelstream.columns:
                modelstream[FN_NEXTDOWNID] = ''
            
            # Create dict to store FN_FROMNODE values for each HydroID
            dnodes=dict()
            lstHydroIDs=[]
            for row in modelstream[[FN_FROMNODE,HYDROID]].iterrows(): 
                
                if (row[1][0] in dnodes)==False:
                    lstHydroIDs=[row[1][1]]
                    dnodes.setdefault(row[1][0],lstHydroIDs)
                else:
                    lstHydroIDs = dnodes[row[1][0]]
                    lstHydroIDs.append(row[1][1])
            
            # for each stream segment, search dict for HydroID downstream and 
            for urow in modelstream[[FN_NEXTDOWNID, FN_TONODE, FN_FROMNODE, HYDROID]].iterrows(): 
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
                modelstream.loc[modelstream[HYDROID]== hydroIDcol,[FN_NEXTDOWNID]] = nextdownIDcol
            
            tReturns = (sOK, modelstream)
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
        modelstream    = args.parameters[0]
        WBD8           = args.parameters[1]
        HYDROID        = args.parameters[2]
        
        oProcessor = BuildStreamTraversalColumns()
        params = (modelstream, WBD8, HYDROID)
        tResults=None
        tResults = oProcessor.execute(params)

        del oProcessor
    except:
        print (str(trace()))
    finally:
        dt = datetime.datetime.now()
        print  ('Finished at ' + dt.strftime("%Y-%m-%d %H:%M:%S"))
