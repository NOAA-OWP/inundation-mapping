## Inundation Mapping: Flood Inundation Mapping for U.S. National Water Model

Flood inundation mapping software configured to work with the U.S. National Water Model operated and maintained by the National Oceanic and Atmospheric Administration (NOAA) National Water Center (NWC).

#### For more information, see the [Inundation Mapping Wiki](https://github.com/NOAA-OWP/inundation-mapping/wiki).

### This folder and files are for unit testing python files

## Creating unit tests

For each python code file that is being tested, unit tests should come in two files: a unit test file (based on the original python code file) and it's json parms file. 

The files should be named following FIM convention:

{source py file name}_unittests.py     ie) derive_level_paths_unittests.py
{source py file name}_params.json      ie) derive_level_paths_params.json


## Tips to creating a new json file for a new unit test file.

There are multiple way to figure out a set of default json parameters for the new unit test file. 

One way is to use the incoming arg parser. Most py files, they include the code block of " __name__ == '__main__':", followed by external arg parsing, ie) the args = vars(parser.parse_args()). 
	- Add a print(args) or similar, and get all the values including keys as output.
	- Copy that into an editor being used to create the json file.
	- Add a line break after every comma.
	- Find/replace all single quotes to double quotes then cleanup the left tab formatting.


## Running unit tests

Start a docker container as you normally would for any development. ie) docker run --rm -it --name <a docker container name> -v /home/<your name>/projects/<folder path>/:/foss_fim {your docker image name}
	- ie) docker run --rm -it --name mytest -v /home/abcd/projects/dev/innudation-mapping/:/foss_fim -v /abcd_share/foss_fim/outputs/:/outputs -v /abcs_share/foss_fim/:/data fim_4:dev_20220208_8eba0ee

For unit tests to work, you need to run the following (if not already in place).
Notice a modified branch "deny_gms_branch_unittests.lst"  (special for unittests)

Here are the params and args you need if you need to re-run unit and branch

gms_run_unit.sh -n fim_unit_test_data_do_not_remove -u "02020005 02030201 05030104" -c /foss_fim/config/params_template.env -j 1 -d /foss_fim/config/deny_gms_unit_default.lst -o -s
	
gms_run_branch.sh -n fim_unit_test_data_do_not_remove -c /foss_fim/config/params_template.env -j 1 -d /foss_fim/config/deny_gms_branch_unittests.lst -o -s

**NOTICE: the deny file used for gms_run_branch... its a special one for unittests `deny_gms_branch_unittests.lst`.

If you need to run inundation tests, fun the following:

python3 foss_fim/tools/synthesize_test_cases.py -c DEV -e GMS -v fim_unit_test_data_do_not_remove -jh 1 -jb 1 -m /outputs/fim_unit_test_data_do_not_remove/alpha_test_metrics.csv -o

If you want to test just one unit test, here is an example:
At the root terminal window, run:  python ./foss_fim/unit_tests/gms/derive_level_paths_unittests.py  or python ./foss_fim/unit_tests/clip_vectors_to_wbd_unittests.py
(replace with your own script and path name)

## Key Notes for creating new unit tests
1) All test functions must start with the phrase "test_". That is how the unit test engine picks it up. The rest of the function name does not have to match the pattern of {function name being tested} but should. Further, the rest of the function name should say what the test is about, ie) _failed_input_path.  ie) test_{some_function_name_from_the_source_code_file}_failed_input_path. It is fine that the function names get very long (common in the industry).

2) The output for a selected "unittest" import engine can be ugly and hard to read. It sometimes mixed outputs from multiple unit test functions simulataneously instead of keeping all output together for a given unit test. We will try to make this better later.

3) If you are using this for development purposes, use caution when checking back in files for unit tests files and json file. If you check it in, it still has to work and work for others and not just for a dev test you are doing.

4) You can not control the order that unit tests are run within a unit test file. (UnitTest engine limitation)

5) There must be at one "{original py file name}_params.json" file.

6) There must be at least one "happy path (successful)" test inside the unittest file. ie) one function that is expected to full pass. You can have multiple "happy path" tests if you want to change values that are fundamentally different, but fully expected to pass. Json files can have multiple nodes, so the default "happy path/success" is suggested to be called "valid_data"

7) Json files can have multiple nodes, so the default "happy path/success" is suggested to be called "valid_data", if one does not already exist. Generally, the individual unit tests, will call the "valid_data" node and override a local method value to a invalid data. In semi-rare, but possible cases, you can add more nodes if you like, but try not to create new Json nodes for a few small field changes, generally only use a new node if there are major and lots of value changes (ie.. major different test conditions).

8) Unit test functions can and should test for all "outputs" from a source function. This includes the functions's return output (if any), but any global variables it might set, and even that saved output files (such as .tif files) have been created and successfully. It is ok to have multiple validation checks (or asserts) in one unit test function.

9) One py file = one "{original py file name}_unittests.py" file.

10) Sometimes you may want to run a full successful "happy path" version through gms_run_by_unit.sh (or similar), to get all of the files you need in place to do your testing. However.. you will want to ensure that none of the outputs are being deleted during the test. One way to solve this is to put in an invalid value for the "-d" parameter (denylist). ie) Normally:  gms_run_unit.sh -n fim_unit_test_data_do_not_remove -u 05030104 -c /foss_fim/config/params_template.env -j 1 -d /foss_fim/config/deny_gms_unit_default.lst -o, but ours would be 
gms_run_unit.sh -n fim_unit_test_data_do_not_remove -u 05030104 -c /foss_fim/config/params_template.env -j 1 -d no_list -o. 

## Future Enhancements
1) We can automate triggers on these files for things like checking triggers or an single global "run_all_unittest" script, but for now.. its one offs.

2) Better output from the unit tests including verbosity output control

3) Over time, it is expected that python code files will be broken down to many functions inside the file. Currently, we tend to have one very large function in each code file which makes unit testing harder and less specific. Generally for each function in a python code file will result in at least one "happy path" unit test function. This might require having test unit test outputs, such as sample tif or small gpkg files in subfolders in the unit tests folder, but this remains to be seen. Note: Our first two files of derive_level_paths_unittests and clip_vectors_to_wbd_unittests are not complete as they do not yet test all output from a method.


## testing for failing conditions
- Over time, you want to start adding functions that specifically look for fail conditions. This is a key part of unit test systems. It is not uncommon to have many dozens of tests functions in one unit test file. Each "fail" type test, must check for `ONLY one variable value change`. Aka.. a "fail" test function should not fundamentally pass in an invalid huc AND an invalid file path.  Those two failing test conditions and must have two seperate unit test functions. 

- It is possible to let a unit test have more than one failed value but only if they are tightly related to trigger just one failure (RARE though). YES.. Over time, we will see TONS of these types of fail unit test functions and they will take a while to run.

- When you create a "fail" test function, you can load up the normal full "params" from the json file, but then you can override (hardcoded) the one (or rarely more than one) variable inside the function. There is a way to "catch" a failure you are expecting, ensure it is the type of failure you expected and make that "failure" to become a true fail, ie) a unit test pass. 

An example is in unit_tests/gms/Derive_level_paths_unittests.py -> test_Derive_level_paths_invalid_input_stream_network (function). It is incomplete but give you the pattern.

We have almost no "assert"s yet, but most unit test usually have one or more "assert" test. See https://docs.python.org/3/library/unittest.html for more details.

## Unit tests currently available
python3 /foss_fim/unit_tests/gms/derive_level_paths_unittests.py
python3 /foss_fim/unit_tests/tools/inundate_unittests.py
python3 /foss_fim/unit_tests/tools/gms_tools/inundate_gms_unittests.py
python3 /foss_fim/unit_tests/clip_vectors_to_wbd_unittests.py
python3 /foss_fim/unit_tests/filter_catchments_and_add_attributes_unittests.py
python3 /foss_fim/unit_tests/rating_curve_comparison_unittests.py
python3 /foss_fim/unit_tests/shared_functions_unittests.py
python3 /foss_fim/unit_tests/split_flows_unittests.py
python3 /foss_fim/unit_tests/usgs_gage_crosswalk_unittests.py


