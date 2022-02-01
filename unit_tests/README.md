## Cahaba: Flood Inundation Mapping for U.S. National Water Model

Flood inundation mapping software configured to work with the U.S. National Water Model operated and maintained by the National Oceanic and Atmospheric Administration (NOAA) National Water Center (NWC).

#### For more information, see the [Cahaba Wiki](https://github.com/NOAA-OWP/cahaba/wiki).

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

Start a docker container as you normally would for any development. ie) docker run --rm -it --name rob_levelpaths -v /home/{your name}/projects/gms_ms_level_paths/fim_4/:/foss_fim {your docker image name}

At the root terminal window, run:  python ./foss_fim/unit_tests/gms/derive_level_paths_unittests.py  or python ./foss_fim/unit_tests/clip_vectors_to_wbd_unittests.py
(replace with your own script and path name)


## Key Notes for creating new unit tests
1) All test functions must start with the phrase "test_". That is how the unit test engine picks it up. The rest of the function name does not have to match the pattern of {function name being tested} but should. Further, the rest of the function name should say what the test is about, ie) _failed_input_path.  ie) test_{some_function_name_from_the_source_code_file}_failed_input_path. It is fine that the function names get very long (common in the industry).

2) The output for a selected "unittest" import engine can be ugly and hard to read. It sometimes mixed outputs from multiple unit test functions simulataneously instead of keeping all output together for a given unit test. We will try to make this better later.

3) If you are using this for development purposes, use caution when checking back in files for unit tests files and json file. If you check it in, it still has to work and work for others and not just for a dev test you are doing.

4) You can not control the order that unit tests are run within a unit test file. (UnitTest engine limitation)

5) There must be at one "{original py file name}_params.json" file.

6) There must be at least one "happy path (successful)" test inside the unittest file. ie) one function that is expected to full pass. You can have multiple "happy path" tests if you want to change values that are fundamentally different, but fully expected to pass.

7) Unit test functions can and should test for all "outputs" from a source function. This includes the functions's return output, but any global variables it might set, and even that saved output files (such as .tif files) have been created and successfully. It is ok to have multiple validation checks (or asserts) in one unit test function.

7) One py file = one "{original py file name}_unittests.py" file.


## Future Enhancements
1) We can automate triggers on these files for things like checking triggers or an single global "run_all_unittest" script, but for now.. its one offs.

2) Better output from the unit tests.

3) We will upgrade it so you can pass in a params.json file to the call to give you even more flexibility by passing in your own params.json (think params_template.json idea)

4) At this time, the root json file has only one "node". We may consider have more than one node in the json file, which has a different set of data for different test conditions. 

5) Over time, it is expected that python code files will be broken down to many functions inside the file. Currently, we tend to have one very large function in each code file which makes unit testing harder and less specific. Generally for each function in a python code file will result in at least one "happy path" unit test function. This might require having test unit test outputs, such as sample tif or small gpkg files in subfolders in the unit tests folder, but this remains to be seen. Note: Our first two files of derive_level_paths_unittests and clip_vectors_to_wbd_unittests are not complete as they do not yet test all output from a method.


## testing for failing conditions
- Over time, you want to start adding functions that specifically look for fail conditions. This is a key part of unit test systems. It is not uncommon to have many dozens of tests functions in one unit test file. Each "fail" type test, must check for `ONLY one variable value change`. Aka.. a "fail" test function should not fundamentally pass in an invalid huc AND an invalid file path.  Those two failing test conditions and must have two seperate unit test functions. 

- It is possible to let a unit test have more than one failed value but only if they are tightly related to trigger just one failure (RARE though). YES.. Over time, we will see TONS of these types of fail unit test functions and they will take a while to run.

- When you create a "fail" test function, you can load up the normal full "params" from the json file, but then you can override (hardcoded) the one (or rarely more than one) variable inside the function. There is a way to "catch" a failure you are expecting, ensure it is the type of failure you expected and make that "failure" to become a true fail, ie) a unit test pass. 

An example is in unit_tests/gms/Derive_level_paths_unittests.py -> test_Derive_level_paths_invalid_input_stream_network (function). It is incomplete but give you the pattern.

We have almost no "assert"s yet, but most unit test usually have one or more "assert" test. See https://docs.python.org/3/library/unittest.html for more details.


