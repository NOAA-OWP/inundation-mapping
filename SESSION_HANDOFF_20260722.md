# Ripple stream filtering session handoff — 2026-07-22

## Working script

`data/ripple/remove_blacklisted_streams_and_ripple_model_domain_gaps.py`

Important: this script was repeatedly overwritten by a stale editor buffer during the session. Reload it from disk before editing, and save/commit the current workspace before replacing the Docker container.

## Implemented behavior

### Included-stream whitelist constraint

`select_valid_streams()` accepts `whitelist_feature_ids`. Its spatial candidates are filtered to whitelist feature IDs before the `within` and `intersects` joins, so non-whitelisted streams cannot enter `included_streams_gdf`, including as topology bridges.

### Final whitelist duplicate and validity logic

- Every occurrence of a repeated `feature_id` is assigned `is_duplicated = True` in `whitelist_final_df`.
- The existing final validity overrides remain in the script.

### Timestamped output names

`RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")`

Every output that previously used date-only `today` now uses the same script-launch timestamp, for example `20260722_1415`.

### `too_long` detection

`flag_too_long_streams()`:

- Uses a spatial-index join instead of scanning every domain for every stream.
- Adds a boolean `too_long` column to a copy of `whitelist_final_df`.
- Evaluates only rows/features with `is_valid == True`.
- Reuses `candidates_metrics_df["not_headwater_stream"]`; it does not recalculate headwaters.
- Excludes every feature classified as any of the following:
  - `included_by == "within"`
  - `topology_bridge == True`
  - `is_bridge == True` in any whitelist row for the feature
- Dissolves domain polygons temporarily by `collection_id` and `model_id`; this does not alter the saved `whitelist_domain_final_gdf`.
- Requires at least two model domains with positive-length stream coverage.
- Requires at least 95% combined coverage.
- Requires every individual model to cover less than 95%.

`process_streams_save_outputs()` passes the already-computed `candidates_metrics_df` into `flag_too_long_streams()` before saving the final whitelist CSV.

## Known examples

- `10836382`: expected `too_long = True`.
  - Two valid whitelist rows.
  - Approximately 97.61% combined coverage.
  - Individual model coverage approximately 60.66% and 36.95%.
- `510675`: expected `too_long = False` because it is a headwater.
- `510735`: expected `too_long = False`.

## Latest validated result

After excluding headwaters, `within`, topology bridges, and `is_bridge` streams:

- 39 unique `too_long` feature IDs
- 56 rows in `whitelist_final_df`

Sorted feature IDs:

```text
482556
484346
510857
511095
513491
513519
515355
515749
516111
516273
516411
516437
516491
517017
517563
2041629
2048033
2600535
2604901
5057845
5058529
6028786
6029370
6030216
7546441
7850627
8546047
8552371
8608347
10055846
10055930
10068675
10835092
10836382
13883032
13888452
13888602
13888852
932030149
```

## Validation performed

- Python syntax compilation passed.
- Synthetic tests confirmed exclusions for headwater, `within`, topology bridge, and `is_bridge` streams.
- Full read-only validation confirmed no excluded feature remained flagged.
- `10836382` remained true and `510675` remained false.
- `whitelist_final_df` inputs were not mutated by the detector.
- `whitelist_domain_final_gdf` inputs were not mutated.
- A recomputed final domain matched the saved 3,450-row, six-column domain exactly, including zero symmetric-difference area.

Validation data used:

- `/outputs/blacklist_metrics_test_20260720/whitelist_ripple_feature_ids_final_20260722.csv`
- `/outputs/blacklist_metrics_test_20260720/whitelist_domain_final_20260722_1001.gpkg`
- `/outputs/blacklist_metrics_test_20260720/all_reaches_sourcemodels_conus.gpkg`

## Pre-existing CLI issue not changed

The argument parser creates keys named `ripple_dir` and `ripple_collections_dir`, while the bottom of the script reads `ripple_analysis_dir` and `ripple_metrics_dir`. A normal command-line run may raise `KeyError` until those argument destinations are corrected. This was deliberately left unchanged because it was outside the requested edits.

## Tomorrow

1. Verify that this handoff file and the script exist outside the disposable container.
2. Reload the script from disk before opening/saving it in an editor.
3. Run `python3 -m py_compile data/ripple/remove_blacklisted_streams_and_ripple_model_domain_gaps.py`.
4. Re-run the pipeline to produce timestamped final outputs and inspect the 39 flagged features.
