from pyspark.sql import SparkSession
from crawler_config.storage_config import SECRET_KEY, ACCESS_KEY, MINIO_ENDPOINT, CRYPTO_NEWS_BUCKET

# Create Spark session with MinIO configurations
spark = (SparkSession.builder
                .appName("MinIO JSON Concatenation")
                .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
                .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
                .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.network.timeout", "600s")
                .config("spark.executor.heartbeatInterval", "60s")
                .config("spark.sql.debug.maxToStringFields", "2000")
            ).getOrCreate()


# Function to read JSON files from MinIO based on the prefix
def read_json_from_minio(prefix: str):
    s3_path = f"s3a://{CRYPTO_NEWS_BUCKET}/{prefix}/*.json"
    df = spark.read.json(s3_path)
    return df

# Function to concatenate JSON files based on prefix
def concatenate_json_files(prefix: str):
    df = read_json_from_minio(prefix)
    # Deduplicate by the 'id' column
    df_dedup = df.dropDuplicates(["id"])

    # Show the first few rows of the deduplicated DataFrame for debugging
    df_dedup.show()

    return df_dedup

# Function to write the result back to MinIO
def write_to_minio(df, output_prefix: str):
    output_path = f"s3a://{CRYPTO_NEWS_BUCKET}/{output_prefix}/"
    df.write.mode("overwrite").json(output_path)

# Main function to run the job
def main():
    import sys
    prefix = sys.argv[1]  
    output_prefix = sys.argv[2]

    # Concatenate the files based on the prefix
    concatenated_df = concatenate_json_files(prefix)
    
    # Optionally write the concatenated DataFrame back to MinIO
    write_to_minio(concatenated_df, output_prefix)

if __name__ == "__main__":
    main()
