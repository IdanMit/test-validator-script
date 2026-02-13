#!/usr/bin/env python3
"""
Filtered Timeline Viewer
Filters and displays timeline events from June 30th to July 2nd
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, lit, to_date
import glob
import os

def create_spark_session():
    """Create a Spark session"""
    return SparkSession.builder \
        .appName("Filtered Timeline Viewer") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def read_and_filter_timeline_files(spark, directory="."):
    """
    Read timeline files and filter events from June 30th to July 2nd
    
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
                
                # Add date column for filtering
                df = df.withColumn("date", to_date(col("timestamp")))
                
                # Filter for June 30th to July 2nd, 2025
                df = df.filter(
                    (col("date") >= "2025-06-30") & 
                    (col("date") <= "2025-07-02")
                )
            
            # Add source file column
            df = df.withColumn("source_file", lit(file_name))
            
            dataframes[file_name] = df
            print(f"  Successfully read {df.count()} records (filtered)")
            
        except Exception as e:
            print(f"  Error reading {file_name}: {str(e)}")
    
    return dataframes

def display_file_data(df, file_name):
    """
    Display the data for a specific file (limit 5 records)
    
    Args:
        df: DataFrame to display
        file_name: Name of the file
    """
    print(f"\n{'='*80}")
    print(f"DISPLAYING DATA FROM: {file_name}")
    print(f"{'='*80}")
    
    # Show file info
    record_count = df.count()
    print(f"Total records in date range: {record_count}")
    
    if record_count == 0:
        print("No events found in the specified date range.")
        return
    
    # Show first 5 records (not truncated)
    print(f"\nFirst 5 records from {file_name}:")
    df.show(5, truncate=False)
    
    print(f"{'='*80}")

def main():
    """Main function"""
    # Create Spark session
    spark = create_spark_session()
    
    try:
        print("Reading and filtering timeline files (June 30th - July 2nd, 2025)...")
        dataframes = read_and_filter_timeline_files(spark)
        
        if not dataframes:
            print("No timeline files found!")
            return
        
        print(f"\nFound {len(dataframes)} timeline files")
        
        # Display each file's data
        for file_name, df in dataframes.items():
            display_file_data(df, file_name)
    
    except Exception as e:
        print(f"Error during execution: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()