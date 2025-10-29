#!/usr/bin/env python3
 
import os
import argparse

"""_summary_
    Overall processing steps (tenatively)
    
    Will call generate_categorical_fim_flows and generate_categorical_fim when applicable.

    1: Start up its own non-shared log system
    
    2: validate the huc is valid and applicable to catfim ??
    
    3: Get list of applicable, valid sites for this HUCs?  from where? master sites metadata or site file?
       Watchign for excluded sites from restricted sites csv.
    
    4: Start a folder structure if not already in place

    5: Create its own sites csv. Populate what we know if anything and continue updating throughout
       processing steps including mapping flags and status data.
       
    6: Load its own metadata, threshold data and flow data, if applicable using shared various files.
    
    7: Various meta and threshold processing? including validation of data ?
    
    8: Figure out stages and if SB also figure out stages.
    
    9: Data adjustments or rejections ? (might be higher or even need more here)
    
    10: If FB, Load branch and HAND data? (rems and hydrotables), liekly all done via inundation scripts
    
    11: Create inundation tifs if applicable and roll them up if branch tifs?
        FB: Call inundation.py ?
        SB: Do our own inundation like we currently do?
    
    12: make extent polys
    
    13: Finalize any data
    
    14: Make final library files for this HUC
    
"""
    
def process_huc():
    print("placeholder")



if __name__ == '__main__':

    '''
    Sample
    python ...
    '''

    # Parse arguments
    parser = argparse.ArgumentParser(description='Run Categorical FIM for a HUC')
    args = vars(parser.parse_args())

    # figure out args
    # no need for pathing other than root catfim run pathing. ie) /data/catfim/test/hand_4_8_x_x_flow_based
    
    # possible args
    
    #   - HUC number (of course)
    
    #   - root catfim output folder (of course)
    
    #   - maybe one for SB versus FB ? - probably
    
    #   - may be some from generate_categorical_fim which will now be pre-processing?

    try:

        # call main program
        process_huc()

    except Exception:
        print("placeholder")
