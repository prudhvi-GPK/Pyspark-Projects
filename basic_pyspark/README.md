# Employee Data Analysis

This project performs data analysis on employee data stored in a PostgreSQL database using PySpark. The analysis includes various aggregations, filtering, and transformations to gain insights into the employee data.

## Prerequisites

- Python 3.x
- Apache Spark
- PostgreSQL
- Java (for Spark)
- Required Python packages: `pyspark`, `os`

## Setup

1. **Install Apache Spark:** Follow the instructions on the [Apache Spark website](https://spark.apache.org/downloads.html) to download and install Spark.
   
2. **Install PostgreSQL:** Follow the instructions on the [PostgreSQL website](https://www.postgresql.org/download/) to download and install PostgreSQL.

3. **Install Required Python Packages:**
   ```bash
   pip install pyspark

## Set Up Environment Variables

Create environment variables for the PostgreSQL username and password:

export DB_USER=your_postgres_username
export DB_PASSWORD=your_postgres_password




## Running the Code

1. **Initialize SparkSession:** The code initializes a SparkSession with the required PostgreSQL JDBC driver.

2. **Load Data:** The employee data is loaded from the PostgreSQL database into a Spark DataFrame.

3. **Data Analysis:**
   - Display the DataFrame.
   - Calculate the average salary.
   - Count the number of employees in each department.
   - Find the highest and lowest salaries.
   - Perform multiple aggregations on the salary column.
   - Filter employees with a salary greater than 70,000.
   - Select specific columns (name and salary).
   - Add a new column for bonus (10% of the salary).
   - Sort the DataFrame by salary in descending order.

4. **Error Handling:** The code includes error handling to catch and print any errors that occur during execution.

5. **Stop SparkSession:** The SparkSession is stopped at the end of the script.
