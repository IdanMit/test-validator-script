#!/usr/bin/env python3
"""
Simple PySpark JSON Reader for Timeline Data
Reads JSON timeline files into PySpark DataFrames
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, lit
import glob
import os

def create_spark_session():
    """Create a Spark session"""
    return SparkSession.builder \
        .appName("Timeline JSON Reader") \
        .getOrCreate()

def read_timeline_files(spark, directory="."):
    """
    Read all timeline JSON files in the directory
    
    Args:
        spark: SparkSession instance
        directory: Directory containing the files
        
    Returns:
        Dictionary of {filename: DataFrame}
    """
    # Find all timeline files
    pattern = os.path.join(directory, "*host-timeline.json")
    timeline_files = glob.glob(pattern)
    
    dataframes = {}
    
    for file_path in timeline_files:
        file_name = os.path.basename(file_path)
        print(f"Reading {file_name}...")
        
        try:
            # Read JSON file
            df = spark.read.json(file_path)
            
            # Convert timestamp if present
            if "@timestamp" in df.columns:
                df = df.withColumn("timestamp", 
                                  from_unixtime(col("@timestamp") / 1000))
            
            # Add source file column
            df = df.withColumn("source_file", lit(file_name))
            
            dataframes[file_name] = df
            print(f"  Successfully read {df.count()} records")
            
        except Exception as e:
            print(f"  Error reading {file_name}: {str(e)}")
    
    return dataframes

def combine_dataframes(dataframes):
    """Combine all DataFrames into one"""
    if not dataframes:
        return None
    
    combined = list(dataframes.values())[0]
    for df in list(dataframes.values())[1:]:
        combined = combined.union(df)
    
    return combined

def main():
    """Main function"""
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read all timeline files
        print("Reading timeline files...")
        dataframes = read_timeline_files(spark)
        
        if not dataframes:
            print("No timeline files found!")
            return
        
        print(f"\nFound {len(dataframes)} timeline files")
        
        # Show info for each DataFrame
        for file_name, df in dataframes.items():
            print(f"\n{file_name}:")
            print(f"  Records: {df.count()}")
            print(f"  Columns: {', '.join(df.columns)}")
            print("  Sample data:")
            df.show(3, truncate=False)
        
        # Combine all DataFrames
        print("\nCombining all DataFrames...")
        combined_df = combine_dataframes(dataframes)
        
        if combined_df:
            print(f"\nCombined DataFrame:")
            print(f"  Total records: {combined_df.count()}")
            print(f"  Columns: {', '.join(combined_df.columns)}")
            
            # Show sample of combined data
            print("  Sample combined data:")
            combined_df.show(5, truncate=False)
            
            # Example analysis
            if "#event_simpleName" in combined_df.columns:
                print("\nEvent type distribution:")
                combined_df.groupBy("#event_simpleName").count() \
                    .orderBy("count", ascending=False) \
                    .show(10, truncate=False)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 