from pyspark.sql import SparkSession
from pyspark.sql.functions import col

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "github_events"
BRONZE_PATH = "/app/data/bronze/github_events"


def main():
    spark = (
        SparkSession.builder
        .appName("GitHub-Kafka-To-Bronze")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("\n========== KAFKA -> BRONZE ==========")

    print("[1] Lecture des messages Kafka...")

    kafka_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    bronze_df = kafka_df.select(
        col("value").cast("string").alias("raw_json"),
        col("topic"),
        col("partition"),
        col("offset")
    )

    total = bronze_df.count()
    print(f"[2] Messages lus : {total}")

    print("[3] Écriture dans la zone Bronze...")

    bronze_df.write.mode("errorifexists").json(BRONZE_PATH)

    saved_df = spark.read.json(BRONZE_PATH)
    saved_count = saved_df.count()

    print("\n========== VALIDATION BRONZE ==========")
    print(f"Messages Kafka : {total}")
    print(f"Lignes Bronze  : {saved_count}")
    print(f"Différence     : {total - saved_count}")

    if total == saved_count:
        print("SUCCESS : Bronze valide !")
    else:
        print("ERROR : différence détectée !")

    saved_df.select(
        "raw_json", "topic", "partition", "offset"
    ).show(3, truncate=100)

    spark.stop()


if __name__ == "__main__":
    main()