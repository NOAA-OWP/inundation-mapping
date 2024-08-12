# Inundation Mapping: Flood Inundation Mapping for U.S. National Water Model

Flood inundation mapping software configured to work with the U.S. National Water Model operated and maintained by the National Oceanic and Atmospheric Administration (NOAA) National Water Center (NWC).

#### For more information, see the [Inundation Mapping Wiki](https://github.com/NOAA-OWP/inundation-mapping/wiki).

# The `/unit_tests` folder contains files for unit testing python files

## Creating unit tests

For each python code file that is being tested, unit tests should come in two files: a unit test file (based on the original python code file) and an accompanying json paramerters file.

The files should be named following FIM convention:

{source py file name}_test.py ->     `derive_level_paths_test.py`
{source py file name}_params.json -> `derive_level_paths_params.json`


## Tips to create a new json file for a new python unit test file.

There are multiple way to figure out a set of default json parameters for the new unit test file.

One way is to use the incoming arg parser. Most python files include the code block of ` __name__ == '__main__':`, followed by external arg parsing (`args = vars(parser.parse_args()`).
* Add a `print(args)` or similar, and get all the values including keys as output.
* Copy that into an editor being used to create the json file.
* Add a line break after every comma.
* Find/replace all single quotes to double quotes then cleanup the left tab formatting.


## Setting up unit test data

You can either pull the unit test data from s3, or generate it:

1.) To pull the unit test data from ESIP's S3 bucket.
```bash
aws s3 cp --recursive \
	s3://noaa-nws-owp-fim/hand_fim/unit_test_data/ \
	<data/outputs/unit_test_data> --no-sign-request
```
The directory which the data is copied into must be named: `unit_test_data`

2.) To generate the required unit test data, following these steps:

* Start a docker container as you normally would for any development.
```bash
docker run --rm -it --name <a docker container name> \
	-v /home/<your name>/projects/<folder path>/:/foss_fim \
	{your docker image name}
```
Example:
```bash
docker run --rm -it --name mytest \
	-v /home/abcd/projects/dev/innudation-mapping/:/foss_fim \
	-v /abcd_share/foss_fim/outputs/:/outputs \
	-v /abcd_share/foss_fim/:/data \
	-v /abcd_share/foss_fim/outputs_temp/:/fim_temp \
	fim_4:dev_20220208_8eba0ee
```
* Call the `fim_pipeline.sh` script with the necessary arguments (`-n` must be `unit_test_data`).
```bash
fim_pipeline.sh -n unit_test_data -u "02020005 05030104" \
	-bd /foss_fim/config/deny_branch_unittests.lst \
	-ud None -j 1 -o
```

__NOTICE:__ the deny file used for fim_pipeline.sh, has a special one for unittests `deny_branch_unittests.lst`.

* If you need to run inundation tests, fun the following:

```bash
python3 foss_fim/tools/synthesize_test_cases.py \
	-c DEV \
	-v unit_test_data \
	-jh 1 -jb 1 \
	-m /data/outputs/unit_test_data/alpha_test_metrics.csv \
	-o
```

## Running unit tests

### If you'd like to test the whole unit test suite:
When inside of the container, ensure you are within the root directory of the repository before running the `pytest` command.
```
cd /foss_fim
pytest /foss_fim/unit_tests
```

__NOTE:__ This is subject to error, as the downloaded/generated data could potentially have a different path than what is specified in the `/unit_tests/*_params.json` files. The data files are not included in this repository, so are subject to change.

### If you want to test just one unit test (from the root terminal window):

```bash
cd /foss_fim
pytest unit_tests/gms/derive_level_paths_test.py
						or
pytest unit_tests/inundate_gms_test.py
```

### If you'd like to run a particular test, you can, for example:
```
pytest -v -s -k test_append_id_to_file_name_single_identifier_success
```

If one test case is choosen, it will scan all of the test files, and scan for the method (test case) specified.

## Key Notes for creating new unit tests
1) All test functions must start with the phrase `test_`. That is how pytest picks them up. The rest of the function name does not have to match the pattern of `function_name_being_tested` but should. Further, the rest of the function name should say what the test is about, ie) `_failed_input_path`.  ie) `test_{some_function_name_from_the_source_code_file}_failed_input_path`. It is fine that the function names get very long (common in the industry).

2) If you are using this for development purposes, use caution when checking back in files for unit tests files and json file. If you check it in, it still has to work and work for others and not just for a dev test you are doing.

3) As of now, you can not control the order that unit tests are run within a unit test file.

4) There must be at least one associated `{original py file name}_params.json` file per unit test.

5) There must be at least one "happy path (successful)" test inside the unittest file. ie) one function that is expected to fully pass. You can have multiple "happy path" tests if you want to change values that are fundamentally different, but fully expected to pass.

6) Json files can have multiple nodes, so the default "happy path/success" is suggested to be called `valid_data`, if one does not already exist. Generally, the individual unit tests, will call the `valid_data` node and override a local method value to a invalid data. In semi-rare, but possible cases, you can add more nodes if you like, but try not to create new Json nodes for a few small field changes, generally only use a new node if there are major and lots of value changes (ie: major different test conditions).

7) Unit test functions can and should test for all "outputs" from a source function. This includes the functions's return output (if any), but any global variables it might set, and even that saved output files (such as .tif files) have been created and successfully. It is ok to have multiple validation checks (or asserts) in one unit test function.

8) One Python file = one `{original py file name}_test.py` file.

9) Sometimes you may want to run a full successful "happy path" version through `fim_pipeline.sh` (or similar), to get all of the files you need in place to do your testing. However, you will want to ensure that none of the outputs are being deleted during the test. One way to solve this is to put in an invalid value for the `-d` parameter (denylist).
ie:
	```bash
	fim_pipeline.sh -n unit_test_data -u 05030104 \
		-c /foss_fim/config/params_template.env -j 1 -d /foss_fim/config/deny_unit_default.lst -o
	```

	 but ours would be:

	```bash
	fim_pipeline.sh -n unit_test_data -u 05030104 \
		-c /foss_fim/config/params_template.env -j 1 -d no_list -o
	```

## [Pytest](https://docs.pytest.org/en/7.2.x/) particulars

The `unit_tests/pyproject.toml` file is used to specify which warnings are disabled for our particular unit tests.

Luckily, `pytest` works well with The Python Standard Library `unittest`. This made the migration of previous unit tests using `unittest` over to `pytest` quite simple. The caveat is that our current unit tests employ elements of both libraries. A full transition to `pytest` will ideally take place at a future date.

## Testing for failing conditions
- Over time, you want to start adding functions that specifically look for fail conditions. This is a key part of unit test systems. It is not uncommon to have many dozens of tests functions in one unit test file. Each "fail" type test, must check for ONLY one variable value change. A "fail" test function should not fundamentally pass in an invalid huc AND an invalid file path.  Those two failing test conditions and must have two seperate unit test functions.

- It is possible to let a unit test have more than one failed value but only if they are tightly related to trigger just one failure (RARE though). YES.. Over time, we will see TONS of these types of fail unit test functions and they will take a while to run.

- When you create a "fail" test function, you can load up the normal full "params" from the json file, but then you can override (hardcoded) the one (or rarely more than one) variable inside the function. There is a way to "catch" a failure you are expecting, ensure it is the type of failure you expected and make that "failure" to become a true fail, ie) a unit test pass.

An example is in `unit_tests/Derive_level_paths_test.py` -> `test_Derive_level_paths_invalid_input_stream_network` (function). This example gives you the pattern implemented in Pytest.

## Future Enhancements
1) Full transition to the `pytest` library, removing classes of `unittest.TestCase` and taking full advantage of available code re-use patterns offered through `pytest`.

2) Over time, it is expected that python files will be broken down to many functions inside the file. Currently, we tend to have one very large function in each python file which makes unit testing harder and less specific. Generally, one function will correlated to at least one "happy path" unit test function. Note: The files `derive_level_paths_test.py` and `clip_vectors_to_wbd_test.py` are not complete as they do not yet test all output from a method.
