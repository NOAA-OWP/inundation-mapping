from run_test_case import test_case


# Create a list of all test_cases for which we have validation data
all_test_cases = test_case.list_all_test_cases(version=fim_version, archive=archive_results, 
        benchmark_categories=[] if benchmark_category == "all" else [benchmark_category])

for test_case_class in all_test_cases:
    agreement_dict = test_case_class.get_current_agreement()
    for mag in agreement_dict:
        for agree in agreement_dict[mag]:

            stats = zonal_stats(os.path.join(test_case_class.fim_dir, "gw_catchments_reaches_filtered_addedAttributes_crosswalked.gpkg"),
                        {"agreement_raster":agree})
######decide if we want one csv per test case (one huc can have many test cases)