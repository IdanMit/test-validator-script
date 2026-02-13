#!/usr/bin/env python3
"""
Clean JSON to CSV Converter for Timeline Data
Converts all JSON timeline files to CSV format with proper file handling
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, lit, to_date
import glob
import os
import shutil

def create_spark_session():
    """Create a Spark session"""
    return SparkSession.builder \
        .appName("Clean CSV Converter") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

def convert_json_to_csv(spark, directory="."):
    """
    Convert all timeline JSON files to CSV format
    
    Args:
        spark: SparkSession instance
        directory: Directory containing the files
    """
    # Find all timeline files
    pattern = os.path.join(directory, "*host-timeline.json")
    timeline_files = glob.glob(pattern)
    
    if not timeline_files:
        print("No timeline files found!")
        return
    
    print(f"Found {len(timeline_files)} timeline files to convert")
    
    for file_path in timeline_files:
        file_name = os.path.basename(file_path)
        csv_file_name = file_name.replace('.json', '.csv')
        temp_dir = f"temp_{csv_file_name}"
        
        print(f"\nConverting {file_name} to {csv_file_name}...")
        
        try:
            # Read JSON file
            df = spark.read.json(file_path)
            
            # Convert timestamp if present
            if "@timestamp" in df.columns:
                df = df.withColumn("timestamp", 
                                  from_unixtime(col("@timestamp") / 1000))
                
                # Add date column for easier filtering
                df = df.withColumn("date", to_date(col("timestamp")))
            
            # Add source file column
            df = df.withColumn("source_file", lit(file_name))
            
            # Get record count
            record_count = df.count()
            print(f"  Records to convert: {record_count}")
            
            if record_count == 0:
                print(f"  No records found in {file_name}, skipping...")
                continue
            
            # Write to temporary directory
            df.write.mode("overwrite").option("header", "true").csv(temp_dir)
            
            # Find the part file and move it to the final location
            part_files = glob.glob(os.path.join(temp_dir, "part-*.csv"))
            if part_files:
                shutil.move(part_files[0], csv_file_name)
                print(f"  Successfully converted to {csv_file_name}")
            else:
                print(f"  Error: No part file found in {temp_dir}")
            
            # Clean up temporary directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            
        except Exception as e:
            print(f"  Error converting {file_name}: {str(e)}")
            # Clean up on error
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

def convert_with_date_filter(spark, directory=".", start_date="2025-06-30", end_date="2025-07-02"):
    """
    Convert JSON files to CSV with date filtering
    
    Args:
        spark: SparkSession instance
        directory: Directory containing the files
        start_date: Start date for filtering (YYYY-MM-DD)
        end_date: End date for filtering (YYYY-MM-DD)
    """
    # Find all timeline files
    pattern = os.path.join(directory, "*host-timeline.json")
    timeline_files = glob.glob(pattern)
    
    if not timeline_files:
        print("No timeline files found!")
        return
    
    print(f"Found {len(timeline_files)} timeline files to convert")
    print(f"Filtering for date range: {start_date} to {end_date}")
    
    for file_path in timeline_files:
        file_name = os.path.basename(file_path)
        csv_file_name = file_name.replace('.json', f'_filtered_{start_date}_to_{end_date}.csv')
        temp_dir = f"temp_{csv_file_name}"
        
        print(f"\nConverting {file_name} to {csv_file_name}...")
        
        try:
            # Read JSON file
            df = spark.read.json(file_path)
            
            # Convert timestamp if present
            if "@timestamp" in df.columns:
                df = df.withColumn("timestamp", 
                                  from_unixtime(col("@timestamp") / 1000))
                
                # Add date column for filtering
                df = df.withColumn("date", to_date(col("timestamp")))
                
                # Filter by date range
                df = df.filter(
                    (col("date") >= start_date) & 
                    (col("date") <= end_date)
                )
            
            # Add source file column
            df = df.withColumn("source_file", lit(file_name))
            
            # Get record count
            record_count = df.count()
            print(f"  Records to convert: {record_count}")
            
            if record_count == 0:
                print(f"  No records found in date range for {file_name}, skipping...")
                continue
            
            # Write to temporary directory
            df.write.mode("overwrite").option("header", "true").csv(temp_dir)
            
            # Find the part file and move it to the final location
            part_files = glob.glob(os.path.join(temp_dir, "part-*.csv"))
            if part_files:
                shutil.move(part_files[0], csv_file_name)
                print(f"  Successfully converted to {csv_file_name}")
            else:
                print(f"  Error: No part file found in {temp_dir}")
            
            # Clean up temporary directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            
        except Exception as e:
            print(f"  Error converting {file_name}: {str(e)}")
            # Clean up on error
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

def cleanup_existing_csv_dirs():
    """Clean up existing CSV directories created by previous runs"""
    csv_dirs = glob.glob("*host-timeline.csv")
    for dir_path in csv_dirs:
        if os.path.isdir(dir_path):
            print(f"Cleaning up existing directory: {dir_path}")
            shutil.rmtree(dir_path)

def main():
    """Main function"""
    # Create Spark session
    spark = create_spark_session()
    
    try:
        print("Clean JSON to CSV Converter for Timeline Data")
        print("=" * 50)
        
        # Clean up existing CSV directories
        cleanup_existing_csv_dirs()
        
        # Ask user for conversion type
        print("\nChoose conversion type:")
        print("1. Convert all data to CSV")
        print("2. Convert filtered data (June 30th - July 2nd, 2025) to CSV")
        
        choice = input("\nEnter your choice (1 or 2): ").strip()
        
        if choice == "1":
            print("\nConverting all timeline files to CSV...")
            convert_json_to_csv(spark)
        elif choice == "2":
            print("\nConverting filtered timeline files to CSV...")
            convert_with_date_filter(spark, start_date="2025-06-30", end_date="2025-07-02")
        else:
            print("Invalid choice. Please run the script again and choose 1 or 2.")
            return
        
        print("\nConversion completed!")
        print("\nGenerated CSV files:")
        csv_files = glob.glob("*.csv")
        for csv_file in csv_files:
            if os.path.isfile(csv_file):  # Only show actual files, not directories
                file_size = os.path.getsize(csv_file)
                print(f"  - {csv_file} ({file_size:,} bytes)")
    
    except Exception as e:
        print(f"Error during execution: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 