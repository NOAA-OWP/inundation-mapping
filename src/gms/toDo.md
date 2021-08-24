# GMS To Do: 
## Immediate
- meta-data for pipeline
- mannings n 0.06 test
- GMS whitelist for FIM 3
- convenience wrapper for fim_run, gms_run_unit.sh, gms_run_branch.sh. Move gms_run_branch.sh and gms_run_unit.sh to src/gms dir
- consider running filter_catchments_and_add_attributes.py in run_by_branch.sh.


## integration
- git rerere save conflict resolutions

## optimize
- 

## Inundation
- recheck polygons and depths

## Evaluations
- synthesize test cases
    - only one item in list for inundate_gms
    - extra file copied to dirs
    - improper masking
- synthesize test cases is spaghetti code. There are 15 indentations at one point.
  The evaluation pipeline currently comprised of run_test_case.py and synthesize_test_cases.py should be completely rewritten to make more modular and flexible. There should be clear functions to run test cases and batches of test cases without dependency on certain file directory or naming structures. For example, run_test_case.py should handle batches of test cases. There should be a command line function to run a test case without hardcoded file paths.
    - having code in the "__main__" part of a script removes it from importing it into other functions. Only embed code specific to command line functionality (argparse, commandline input handling, and main function call) in the main part. Embed everything else in the functions to use in other scripts.
