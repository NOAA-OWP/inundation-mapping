import pytest 

@pytest.fixture
def get_params_filename(unit_test_file_name):
   
    unittest_file_name = os.path.basename(unit_test_file_name)
    params_file_name = unittest_file_name.replace("_test.py", "_params.json")
    params_file_path = os.path.join(os.path.dirname(unit_test_file_name), params_file_name)
    
    if (not os.path.exists(params_file_path)):
        raise FileNotFoundError(f"{params_file_path} does not exist")
    
    return params_file_path