## FIM in a Box

### Summary
Contains files to create a self-contained FIM sandbox demo image, including:
1. generate data to run and evaluate FIM (`fim_pipeline.sh` and `synthesize_test_cases.py`) for a single HUC
2. create a Docker image that contains the data generated in the previous step in `/data` and a copy of the `inundation-mapping` code in `/foss_fim`

### Usage
An example workflow for these files is as follows:
1. In an `inundation-mapping` Docker container, run `get_sample_data.py` to generate sample `input` and `test_cases` data. The `-s3` flag can be used to download from an AWS S3 bucket; access keys must be provided. Note: `get_sample_data.py` currently doesn't support HUCs in Alaska.
```
/foss_fim/data/sandbox/get_sample_data.py -u 03100204 -i /data -o /foss_fim/data/sandbox/sample-data
```

2. From the root of the `inundation-mapping` repository, copy `Pipfile`, `Pipfile.lock`, and `entrypoint.sh` to the `sandbox` folder:
```
cp Pipfile* data/sandbox/
cp entrypoint.sh data/sandbox/
```

3. Build the Docker image. This assumes that the `sample-data` folder is located in the same folder as the `/foss_fim/data/sandbox/Dockerfile`. The current `inundation-mapping` repository will be copied to `/foss_fim` and the `sample-data` folder will be copied to `/data`. 
```
docker build --rm --no-cache --force-rm -t fim_4:4.5.2.1-sandbox .
```

4. Run the Docker container.
NOTES:
- If you want the outputs to persist (i.e., written to disk instead of being erased when the container is exited), `/outputs` can be mounted to a local path by including `-v [local/path]:/outputs` in the `docker run` command below.
```
docker run --rm -it --name sandbox fim_4:4.5.2.1-sandbox
```

5. Generate FIM hydrofabric (e.g., HAND rasters). The results will be saved in `/outputs`.
```
fim_pipeline.sh -u 03100204 -jh 1 -jb 5 -n sandbox_run
```

6. Evaluate against benchmark data. The `-b` argument must be followed by whichever benchmark is desired to be evaluated against (e.g., `nws`, `usgs`, or `ble`). All benchmarks do not exist for all HUCs, so the benchmark data must exist for the desired HUC or the evaluation will fail.
```
/foss_fim/tools/synthesize_test_cases.py -c DEV -jh 1 -jb 5 -vr -vg -o -v sandbox_run -b nws
```
