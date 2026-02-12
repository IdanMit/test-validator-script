#!/usr/bin/env python3
"""
Example Analysis Script for Timeline Data
Demonstrates practical analysis of the JSON timeline data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, count, desc, when, regexp_extract
from simple_json_reader import read_timeline_files, combine_dataframes

def analyze_process_events(df):
    """Analyze process-related events"""
    print("\n=== Process Event Analysis ===")
    
    # Filter for process events
    process_events = df.filter(col("#event_simpleName") == "ProcessRollup2")
    
    print(f"Total process events: {process_events.count()}")
    
    # Extract process names from ImageFileName
    process_analysis = process_events.withColumn(
        "process_name", 
        regexp_extract(col("ImageFileName"), r"\\(\w+\.exe)$", 1)
    )
    
    # Top processes by count
    print("\nTop 10 processes by execution count:")
    process_analysis.groupBy("process_name").count() \
        .orderBy("count", ascending=False) \
        .show(10, truncate=False)
    
    # Processes by user
    print("\nProcess events by user:")
    process_events.groupBy("UserName").count() \
        .orderBy("count", ascending=False) \
        .show(10, truncate=False)

def analyze_file_events(df):
    """Analyze file-related events"""
    print("\n=== File Event Analysis ===")
    
    # Filter for file events
    file_events = df.filter(col("#event_simpleName").isin(["PeVersionInfo", "NewExecutableRenamed"]))
    
    print(f"Total file events: {file_events.count()}")
    
    # Extract file extensions
    file_analysis = file_events.withColumn(
        "file_extension", 
        regexp_extract(col("ImageFileName"), r"\.(\w+)$", 1)
    )
    
    # File types by count
    print("\nFile types by count:")
    file_analysis.groupBy("file_extension").count() \
        .orderBy("count", ascending=False) \
        .show(10, truncate=False)

def analyze_timeline_patterns(df):
    """Analyze temporal patterns"""
    print("\n=== Timeline Pattern Analysis ===")
    
    # Events over time (hourly)
    hourly_events = df.withColumn("hour", col("timestamp").substr(12, 2))
    
    print("Events by hour of day:")
    hourly_events.groupBy("hour").count() \
        .orderBy("hour") \
        .show(24, truncate=False)
    
    # Events by day
    daily_events = df.withColumn("date", col("timestamp").substr(1, 10))
    
    print("\nEvents by date:")
    daily_events.groupBy("date").count() \
        .orderBy("date") \
        .show(10, truncate=False)

def analyze_security_events(df):
    """Analyze security-related events"""
    print("\n=== Security Event Analysis ===")
    
    # Look for suspicious patterns
    suspicious_events = df.filter(
        (col("ImageFileName").contains("cmd.exe")) |
        (col("ImageFileName").contains("powershell.exe")) |
        (col("ImageFileName").contains("wmic.exe")) |
        (col("ImageFileName").contains("net.exe"))
    )
    
    print(f"Potentially suspicious events: {suspicious_events.count()}")
    
    if suspicious_events.count() > 0:
        print("\nSuspicious command executions:")
        suspicious_events.select("UserName", "ImageFileName", "timestamp", "Attributes") \
            .orderBy("timestamp") \
            .show(10, truncate=False)

def analyze_host_activity(df):
    """Analyze activity by host"""
    print("\n=== Host Activity Analysis ===")
    
    # Activity by host (aid)
    host_activity = df.groupBy("aid").agg(
        count("*").alias("total_events"),
        count(when(col("#event_simpleName") == "ProcessRollup2", True)).alias("process_events"),
        count(when(col("#event_simpleName") == "PeVersionInfo", True)).alias("file_events")
    ).orderBy("total_events", ascending=False)
    
    print("Host activity summary:")
    host_activity.show(10, truncate=False)

def main():
    """Main analysis function"""
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Timeline Analysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Read timeline files
        print("Reading timeline files...")
        dataframes = read_timeline_files(spark)
        
        if not dataframes:
            print("No timeline files found!")
            return
        
        # Combine DataFrames
        combined_df = combine_dataframes(dataframes)
        
        if not combined_df:
            print("No data to analyze!")
            return
        
        print(f"\nAnalyzing {combined_df.count()} total events...")
        
        # Perform various analyses
        analyze_process_events(combined_df)
        analyze_file_events(combined_df)
        analyze_timeline_patterns(combined_df)
        analyze_security_events(combined_df)
        analyze_host_activity(combined_df)
        
        # Summary statistics
        print("\n=== Summary Statistics ===")
        print(f"Total events: {combined_df.count()}")
        print(f"Unique hosts: {combined_df.select('aid').distinct().count()}")
        print(f"Event types: {combined_df.select('#event_simpleName').distinct().count()}")
        
        # Event type breakdown
        print("\nEvent type breakdown:")
        combined_df.groupBy("#event_simpleName").count() \
            .orderBy("count", ascending=False) \
            .show(truncate=False)
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 