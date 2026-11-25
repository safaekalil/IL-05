
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, get_json_object, to_timestamp,
    to_date, date_format, unix_timestamp,
    row_number, count
)
from pyspark.sql.window import Window

BRONZE_PATH = "/app/data/bronze/github_events"
SILVER_PATH = "/app/data/silver/github_events_hudi"
TABLE_NAME = "github_events_silver"

SUPPORTED_TYPES = [
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent"
]


def main():
    spark = (
        SparkSession.builder
        .appName("Bronze-To-Silver-Hudi")
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("\n========== BRONZE -> SILVER ==========")

    # 1. Lire les données Bronze
    bronze_df = spark.read.json(BRONZE_PATH)
    bronze_count = bronze_df.count()
    print(f"[1] Lignes Bronze : {bronze_count}")

    # 2. Extraire les champs depuis le JSON brut
    extracted_df = bronze_df.select(
        get_json_object("raw_json", "$.id").alias("event_id"),
        get_json_object("raw_json", "$.type").alias("event_type"),
        get_json_object("raw_json", "$.repo.name").alias("repo_name"),
        get_json_object("raw_json", "$.actor.login").alias("actor_login"),
        get_json_object("raw_json", "$.created_at").alias("created_at"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset")
    )

    # 3. Convertir la date et vérifier les champs obligatoires
    parsed_df = extracted_df.withColumn(
        "event_ts", to_timestamp("created_at")
    )

    valid_condition = (
        col("event_id").isNotNull() &
        col("event_type").isNotNull() &
        col("repo_name").isNotNull() &
        col("actor_login").isNotNull() &
        col("event_ts").isNotNull()
    )

    invalid_count = parsed_df.filter(~valid_condition).count()
    valid_df = parsed_df.filter(valid_condition)

    print(f"[2] Événements invalides : {invalid_count}")

    # 4. Compter les types d'événements non retenus
    unsupported_df = (
        valid_df
        .filter(~col("event_type").isin(SUPPORTED_TYPES))
        .groupBy("event_type")
        .agg(count("*").alias("total"))
        .orderBy(col("total").desc())
    )

    print("[3] Types d'événements ignorés :")
    unsupported_df.show(50, truncate=False)

    unsupported_count = (
        valid_df
        .filter(~col("event_type").isin(SUPPORTED_TYPES))
        .count()
    )

    # 5. Garder uniquement les trois types demandés
    filtered_df = valid_df.filter(
        col("event_type").isin(SUPPORTED_TYPES)
    )

    filtered_count = filtered_df.count()
    print(f"[4] Événements après filtrage : {filtered_count}")

    # 6. Supprimer les doublons par event_id
    # Priorité : timestamp, partition Kafka, offset Kafka
    window_spec = (
        Window.partitionBy("event_id")
        .orderBy(
            col("event_ts").desc(),
            col("kafka_partition").desc(),
            col("kafka_offset").desc()
        )
    )

    deduplicated_df = (
        filtered_df
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    deduplicated_count = deduplicated_df.count()
    duplicates_count = filtered_count - deduplicated_count

    print(f"[5] Doublons supprimés : {duplicates_count}")
    print(f"[6] Événements uniques : {deduplicated_count}")

    # 7. Construire le schéma final Silver
    silver_df = deduplicated_df.select(
        col("event_id").cast("string"),
        col("event_type").cast("string"),
        col("repo_name").cast("string"),
        col("actor_login").cast("string"),
        col("event_ts"),
        date_format(
            to_date("event_ts"), "yyyy-MM-dd"
        ).alias("event_date"),
        (unix_timestamp("event_ts") * 1000)
            .cast("long").alias("event_epoch_ms")
    ).withColumn(
        "repo_language_observed",
        col("event_id").cast("string")
    )

    # La langue n'est pas fournie directement par
    # les événements GH Archive : elle reste inconnue.
    from pyspark.sql.functions import lit
    silver_df = silver_df.withColumn(
        "repo_language_observed", lit(None).cast("string")
    )

    # 8. Écriture Apache Hudi : COPY_ON_WRITE + UPSERT
    print("[7] Écriture de la table Apache Hudi...")

    hudi_options = {
        "hoodie.table.name": TABLE_NAME,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "event_id",
        "hoodie.datasource.write.partitionpath.field": "event_date",
        "hoodie.datasource.write.precombine.field": "event_epoch_ms",
        "hoodie.datasource.write.keygenerator.class":
            "org.apache.hudi.keygen.SimpleKeyGenerator",
        "hoodie.datasource.write.hive_style_partitioning": "true"
    }

    (
        silver_df.write
        .format("hudi")
        .options(**hudi_options)
        .mode("append")
        .save(SILVER_PATH)
    )

    # 9. Relire et valider la table Silver
    result_df = spark.read.format("hudi").load(SILVER_PATH)

    silver_count = result_df.count()
    distinct_count = result_df.select("event_id").distinct().count()

    print("\n========== VALIDATION SILVER ==========")
    print(f"Bronze total             : {bronze_count}")
    print(f"Événements invalides     : {invalid_count}")
    print(f"Types non retenus        : {unsupported_count}")
    print(f"Événements filtrés       : {filtered_count}")
    print(f"Doublons supprimés       : {duplicates_count}")
    print(f"Lignes Silver            : {silver_count}")
    print(f"Identifiants distincts   : {distinct_count}")

    print("\nSchéma Silver :")
    result_df.printSchema()

    print("\nExemples Silver :")
    result_df.select(
        "event_id", "event_type", "repo_name",
        "actor_login", "event_ts", "event_date"
    ).show(5, truncate=False)

    if silver_count == distinct_count:
        print("SUCCESS : validation Silver réussie !")
    else:
        print("ERROR : des doublons subsistent dans Silver.")

    spark.stop()


if __name__ == "__main__":
    main()
