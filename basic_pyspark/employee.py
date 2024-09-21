from pyspark.sql import SparkSession
import os

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Employee Data Analysis") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
    .getOrCreate()

# PostgreSQL connection properties
jdbc_url = "jdbc:postgresql://localhost:5432/sample"
connection_properties = {
    "user": "postgres",
    "password": "Luffy10$",
    "driver": "org.postgresql.Driver"
}

try:
    # Load data into DataFrame
    df = spark.read.jdbc(url=jdbc_url, table="employee", properties=connection_properties)
    
    # Show DataFrame
    df.show()
    
    # Calculate average salary
    avg_salary = df.groupBy().avg("salary").collect()[0][0]
    print(f"Average Salary: {avg_salary}")
    
    # Count employees in each department
    df.groupBy("department").count().show()
    
    # Find highest and lowest salaries
    df.agg({"salary": "max"}).show()
    df.agg({"salary": "min"}).show()
    
    # Perform multiple aggregations
    agg_result = df.agg(
        {"salary": "avg"},
        {"salary": "max"},
        {"salary": "min"},
        {"salary": "sum"},
        {"salary": "stddev"},
        {"salary": "variance"},
        {"salary": "skewness"},
        {"salary": "kurtosis"},
        {"salary": "count"},
        {"salary": "approx_count_distinct"},
        {"salary": "first"},
        {"salary": "last"}
    )
    
    # Show aggregation results
    agg_result.show()
    
    # Filter employees with salary greater than 70,000
    high_salary_df = df.filter(df["salary"] > 70000)
    high_salary_df.show()
    
    # Select specific columns (name and salary)
    selected_df = df.select("name", "salary")
    selected_df.show()
    
    # Add a new column for bonus, which is 10% of the salary
    bonus_df = df.withColumn("bonus", df["salary"] * 0.10)
    bonus_df.show()
    
    # Sort by salary in descending order
    sorted_df = df.orderBy(df["salary"].desc())
    sorted_df.show()

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Stop SparkSession
    spark.stop()
