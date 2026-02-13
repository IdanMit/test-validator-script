#!/usr/bin/env python3
"""
JSON to CSV Converter for Timeline Data
Converts all JSON timeline files to CSV format
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, lit, to_date
import glob
import os

def create_spark_session():
    """Create a Spark session"""
    return SparkSession.builder \
        .appName("JSON to CSV Converter") \
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
        csv_file_path = os.path.join(directory, csv_file_name)
        
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
            
            # Write to CSV
            df.write.mode("overwrite").option("header", "true").csv(csv_file_path)
            
            # Rename the part file to the actual CSV file
            part_files = glob.glob(os.path.join(csv_file_path, "part-*.csv"))
            if part_files:
                import shutil
                shutil.move(part_files[0], csv_file_path)
                # Remove the temporary directory
                shutil.rmtree(csv_file_path)
            
            print(f"  Successfully converted to {csv_file_name}")
            
        except Exception as e:
            print(f"  Error converting {file_name}: {str(e)}")

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
        csv_file_path = os.path.join(directory, csv_file_name)
        
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
            
            # Write to CSV
            df.write.mode("overwrite").option("header", "true").csv(csv_file_path)
            
            # Rename the part file to the actual CSV file
            part_files = glob.glob(os.path.join(csv_file_path, "part-*.csv"))
            if part_files:
                import shutil
                shutil.move(part_files[0], csv_file_path)
                # Remove the temporary directory
                shutil.rmtree(csv_file_path)
            
            print(f"  Successfully converted to {csv_file_name}")
            
        except Exception as e:
            print(f"  Error converting {file_name}: {str(e)}")

def main():
    """Main function"""
    # Create Spark session
    spark = create_spark_session()
    
    try:
        print("JSON to CSV Converter for Timeline Data")
        print("=" * 50)
        
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
            print(f"  - {csv_file}")
    
    except Exception as e:
        print(f"Error during execution: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 