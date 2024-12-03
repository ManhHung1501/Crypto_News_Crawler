from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from crawler_config.storage_config import SECRET_KEY, ACCESS_KEY, MINIO_ENDPOINT, CRYPTO_NEWS_BUCKET

# Create Spark session with MinIO configurations
spark = (SparkSession.builder
                .appName("MinIO JSON Concatenation")
                .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
                .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.network.timeout", "600s")
                .config("spark.executor.heartbeatInterval", "60s")
                .config("spark.sql.debug.maxToStringFields", "2000")
            ).getOrCreate()


# Function to read JSON files from MinIO based on the prefix
def read_json_from_minio(partern: str):
    schema = StructType([
        StructField("content", StringType(), True),
        StructField("id", StringType(), True),
        StructField("published_at", StringType(), True),
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("url", StringType(), True)
    ])
    s3_path = f"s3a://{CRYPTO_NEWS_BUCKET}/{partern}"
    df = spark.read.schema(schema).option("mode", "FAILFAST").json(
        path=s3_path,
        encoding="UTF-8",
        multiLine=True,
        recursiveFileLookup=True
    )
    print(f"Reading {len(df.inputFiles())} from {partern}")
    return df

# Function to concatenate JSON files based on partern
def concatenate_json_files(partern: str):
    df = read_json_from_minio(partern)
    # Deduplicate by the 'id' column
    df_dedup = df.dropDuplicates(["id"])

    # Show the first few rows of the deduplicated DataFrame for debugging
    df_dedup.show()

    return df_dedup

# Function to write the result back to MinIO
def write_to_minio(df, output_path: str):
    output_path = f"s3a://{CRYPTO_NEWS_BUCKET}/{output_path}"
    df.write.mode("append").json(output_path)

# Main function to run the job
def main():
    import sys
    partern = sys.argv[1]  
    output_path = sys.argv[2]
    print(f"partern: {partern}")
    print(f"output_path: {output_path}")

    # Concatenate the files based on the prefix
    concatenated_df = concatenate_json_files(partern)
    
    
    # Optionally write the concatenated DataFrame back to MinIO
    write_to_minio(concatenated_df, output_path)

if __name__ == "__main__":
    main()
