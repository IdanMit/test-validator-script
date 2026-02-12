#!/usr/bin/env python3
"""
PySpark JSON Reader for Timeline Data
This script reads multiple JSON timeline files into PySpark DataFrames
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp, lit
import glob
import os
from typing import List, Dict, Any

def create_spark_session(app_name: str = "JSON Timeline Reader") -> SparkSession:
    """
    Create and configure a Spark session
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        Configured SparkSession
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .getOrCreate()

def get_timeline_files(directory: str = ".") -> List[str]:
    """
    Get all timeline JSON files in the specified directory
    
    Args:
        directory: Directory to search for files
        
    Returns:
        List of timeline file paths
    """
    pattern = os.path.join(directory, "*host-timeline.json")
    return glob.glob(pattern)

def read_json_file(spark: SparkSession, file_path: str) -> 'DataFrame':
    """
    Read a single JSON file into a DataFrame
    
    Args:
        spark: SparkSession instance
        file_path: Path to the JSON file
        
    Returns:
        DataFrame containing the JSON data
    """
    print(f"Reading file: {file_path}")
    
    # Read JSON file
    df = spark.read.json(file_path)
    
    # Add source file information
    file_name = os.path.basename(file_path)
    df = df.withColumn("source_file", lit(file_name))
    
    # Convert timestamp columns
    if "@timestamp" in df.columns:
        df = df.withColumn("timestamp", 
                          from_unixtime(col("@timestamp") / 1000))
    
    if "timestamp_UTC_readable" in df.columns:
        df = df.withColumn("timestamp_parsed", 
                          to_timestamp(col("timestamp_UTC_readable")))
    
    return df

def read_all_timeline_files(spark: SparkSession, directory: str = ".") -> Dict[str, 'DataFrame']:
    """
    Read all timeline files and return a dictionary of DataFrames
    
    Args:
        spark: SparkSession instance
        directory: Directory containing timeline files
        
    Returns:
        Dictionary mapping file names to DataFrames
    """
    timeline_files = get_timeline_files(directory)
    dataframes = {}
    
    for file_path in timeline_files:
        file_name = os.path.basename(file_path)
        try:
            df = read_json_file(spark, file_path)
            dataframes[file_name] = df
            print(f"Successfully read {file_name}: {df.count()} records")
        except Exception as e:
            print(f"Error reading {file_name}: {str(e)}")
    
    return dataframes

def combine_all_dataframes(dataframes: Dict[str, 'DataFrame']) -> 'DataFrame':
    """
    Combine all DataFrames into a single DataFrame
    
    Args:
        dataframes: Dictionary of DataFrames
        
    Returns:
        Combined DataFrame
    """
    if not dataframes:
        return None
    
    # Get the first DataFrame
    combined_df = list(dataframes.values())[0]
    
    # Union all other DataFrames
    for df in list(dataframes.values())[1:]:
        combined_df = combined_df.union(df)
    
    return combined_df

def analyze_dataframe(df: 'DataFrame', name: str = "DataFrame"):
    """
    Perform basic analysis on a DataFrame
    
    Args:
        df: DataFrame to analyze
        name: Name for the DataFrame
    """
    print(f"\n=== Analysis for {name} ===")
    print(f"Total records: {df.count()}")
    print(f"Columns: {', '.join(df.columns)}")
    
    # Show schema
    print("\nSchema:")
    df.printSchema()
    
    # Show sample data
    print("\nSample data:")
    df.show(5, truncate=False)
    
    # Count by event type if available
    if "#event_simpleName" in df.columns:
        print("\nEvent type distribution:")
        df.groupBy("#event_simpleName").count().orderBy("count", ascending=False).show(10, truncate=False)

def main():
    """
    Main function to demonstrate the JSON reading functionality
    """
    # Create Spark session
    spark = create_spark_session("Timeline JSON Reader")
    
    try:
        # Read all timeline files
        print("Reading timeline files...")
        dataframes = read_all_timeline_files(spark)
        
        if not dataframes:
            print("No timeline files found!")
            return
        
        print(f"\nFound {len(dataframes)} timeline files")
        
        # Analyze each DataFrame
        for file_name, df in dataframes.items():
            analyze_dataframe(df, file_name)
        
        # Combine all DataFrames
        print("\nCombining all DataFrames...")
        combined_df = combine_all_dataframes(dataframes)
        
        if combined_df:
            analyze_dataframe(combined_df, "Combined Timeline Data")
            
            # Example queries
            print("\n=== Example Queries ===")
            
            # Count total events
            total_events = combined_df.count()
            print(f"Total events across all files: {total_events}")
            
            # Count by event type
            if "#event_simpleName" in combined_df.columns:
                print("\nTop 10 event types:")
                combined_df.groupBy("#event_simpleName").count() \
                    .orderBy("count", ascending=False) \
                    .show(10, truncate=False)
            
            # Count by host (aid)
            if "aid" in combined_df.columns:
                print("\nEvents by host (aid):")
                combined_df.groupBy("aid").count() \
                    .orderBy("count", ascending=False) \
                    .show(5, truncate=False)
            
            # Time range analysis
            if "timestamp" in combined_df.columns:
                print("\nTime range:")
                combined_df.select("timestamp").summary("min", "max").show()
        
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main() 