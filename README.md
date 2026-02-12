# PySpark JSON Timeline Reader

This project provides PySpark scripts to read JSON timeline files into DataFrames for analysis.

## Features

- **Dynamic file discovery**: Automatically finds all `*host-timeline.json` files in a directory
- **Timestamp conversion**: Converts Unix timestamps to readable datetime format
- **DataFrame combination**: Combines multiple files into a single DataFrame for analysis
- **Error handling**: Gracefully handles file reading errors
- **Sample analysis**: Provides basic data analysis examples

## Requirements

- Python 3.7+
- PySpark 3.4.0+

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure you have Java 8+ installed (required for PySpark)

## Usage

### Basic Usage

Run the simple reader:
```bash
python simple_json_reader.py
```

Run the advanced reader with detailed analysis:
```bash
python pyspark_json_reader.py
```

### Programmatic Usage

```python
from pyspark.sql import SparkSession
from simple_json_reader import read_timeline_files, combine_dataframes

# Create Spark session
spark = SparkSession.builder.appName("My App").getOrCreate()

# Read all timeline files
dataframes = read_timeline_files(spark, directory=".")

# Combine into single DataFrame
combined_df = combine_dataframes(dataframes)

# Perform analysis
if combined_df:
    # Count total events
    total_events = combined_df.count()
    print(f"Total events: {total_events}")
    
    # Group by event type
    event_counts = combined_df.groupBy("#event_simpleName").count()
    event_counts.show()

spark.stop()
```

## File Structure

The scripts expect JSON files with the following structure:
```json
[
  {
    "UserName": "--",
    "aid": "d5496d25056644609ffb80e6eea76430",
    "Attributes": "File Name: ...",
    "DomainName": "--",
    "PatternId": "--",
    "TargetFileName": "--",
    "@timestamp": 1751392944778,
    "TreeId": "--",
    "ParentProcessId": "--",
    "ImageFileName": "...",
    "UserSid": "--",
    "DestIP": "--",
    "timestamp_UTC_readable": "2025-07-01T18:02:24+0000",
    "ProcessId": "--",
    "#event_simpleName": "PeVersionInfo"
  }
]
```

## Output

The scripts will:
1. Read all timeline files in the current directory
2. Convert timestamps to readable format
3. Combine all DataFrames
4. Display sample data and basic statistics
5. Show event type distribution

## Example Output

```
Reading timeline files...
Reading EAG-CRP-ADMIN1host-timeline.json...
  Successfully read 15000 records
Reading AVC-CRP-DC10host-timeline.json...
  Successfully read 12000 records

Found 2 timeline files

EAG-CRP-ADMIN1host-timeline.json:
  Records: 15000
  Columns: UserName, aid, Attributes, DomainName, PatternId, TargetFileName, @timestamp, TreeId, ParentProcessId, ImageFileName, UserSid, DestIP, timestamp_UTC_readable, ProcessId, #event_simpleName, timestamp, source_file
  Sample data:
  +--------+--------------------+--------------------+-----------+----------+---------------+-------------+------+---------------+--------------------+--------+-----+------------------------+----------+------------------+-------------------+-----------+
  |UserName|aid                 |Attributes          |DomainName |PatternId|TargetFileName |@timestamp   |TreeId|ParentProcessId|ImageFileName       |UserSid |DestIP|timestamp_UTC_readable|ProcessId |#event_simpleName |timestamp         |source_file|
  +--------+--------------------+--------------------+-----------+----------+---------------+-------------+------+---------------+--------------------+--------+-----+------------------------+----------+------------------+-------------------+-----------+
  |--      |d5496d25056644609...|File Name: \Device...|--         |--        |--             |1751392944778|--    |--             |\Device\HarddiskV...|--      |--    |2025-07-01T18:02:24+0000|--        |PeVersionInfo     |2025-07-01 18:02:24|null       |
  +--------+--------------------+--------------------+-----------+----------+---------------+-------------+------+---------------+--------------------+--------+-----+------------------------+----------+------------------+-------------------+-----------+

Combined DataFrame:
  Total records: 27000
  Columns: UserName, aid, Attributes, DomainName, PatternId, TargetFileName, @timestamp, TreeId, ParentProcessId, ImageFileName, UserSid, DestIP, timestamp_UTC_readable, ProcessId, #event_simpleName, timestamp, source_file

Event type distribution:
+------------------+-----+
|#event_simpleName |count|
+------------------+-----+
|ProcessRollup2    |15000|
|PeVersionInfo     |8000 |
|NewExecutableRenamed|2500 |
|RansomwareOpenFile|1000 |
|DiskCapacity      |500  |
+------------------+-----+
```

## Validators and CI

Required checks run on pull requests and pushes to `main`/`master`. Run them locally before pushing.

### Validators

| Validator | Script | CI job | Purpose |
|-----------|--------|--------|---------|
| Tests required | `validate_test_required.py` | `validate:test-required` | Ensures tests exist for source code and that tests pass |

### Run validators locally

```bash
# Install dependencies (includes pytest)
pip install -r requirements.txt

# Run test-required validator (checks tests exist + runs pytest)
python validate_test_required.py
```

**Exit codes:** `0` = pass, `1` = fail. On failure the script prints **what failed**, **why**, and **how to fix**.

### Fail messaging

- **"No tests found for any source module"** — Add at least one test under `tests/` (e.g. `tests/test_<module>.py`) for a source module.
- **"Tests failed or did not run"** — Run `python -m pytest .` locally and fix failing tests.
- **"pytest not found"** — Run `pip install pytest` (or `pip install -r requirements.txt`).

To skip validation with justification (use sparingly): set `VALIDATION_SKIP=reason` in CI or locally.

### Optional: env audit

CI records `printenv` to `.validator-env.log` and uploads it as an artifact for compliance; the file is gitignored.

## Troubleshooting

### Common Issues

1. **Java not found**: Ensure Java 8+ is installed and `JAVA_HOME` is set
2. **Memory issues**: For large files, increase Spark memory settings:
   ```python
   spark = SparkSession.builder \
       .appName("Timeline Reader") \
       .config("spark.driver.memory", "4g") \
       .config("spark.executor.memory", "4g") \
       .getOrCreate()
   ```
3. **File not found**: Ensure timeline files are in the current directory or specify the correct path

### Performance Tips

- For very large files, consider using Spark's partitioning
- Use `coalesce()` to reduce the number of partitions if needed
- Consider caching frequently used DataFrames with `.cache()`

## License

This project is open source and available under the MIT License. 