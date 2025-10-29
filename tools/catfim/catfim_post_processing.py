import os
import argparse

"""_summary_
    Overall processing steps (tenatively)
    
    Should not need to call any other of the catfim py files. Some of those master rollup 
    functions should be moved here. Should not need to update the master sites or library files,
    only append them.
    
    1: Start up its own non-shared log system
    
    2: Validate HUCs data (has some sies and library data remaining)
    
    3: roll up all HUC level sites.csv/gpkg's and library files csv/gpkg.
    
    4: Roll up HUC logs?  not sure about that one.
    
    5: Roll up HUC error/warning logs?  seperate logs for warnign versus error?
    
"""

def catfim_post_processing():
    print("placeholder")
    
    

if __name__ == '__main__':

    '''
    Sample
    python ...
    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Run Post Processing for CatFIM HUCs ?? ')
    args = vars(parser.parse_args())

    # figure out args
    # no need for pathing other than root catfim run pathing. ie) /data/catfim/test/hand_4_8_x_x_flow_based
    
    # possible args
    #   - root catfim output folder (of course)

    try:

        # call main program
        catfim_post_processing()

    except Exception:
        print("placeholder")
