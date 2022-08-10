from pyspark.sql import SparkSession, dataframe
from pyspark.sql import functions as F
from decouple import config


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", config("DIR_POSTGRES_JAR_FILE")) \
    .getOrCreate()

connection = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{config('HOST')}:{config('PORT')}/{config('DATABASE')}") \
    .option("user", config('BD_USER')) \
    .option("password", config('BD_PASSWORD')) \
    .option("driver", "org.postgresql.Driver")


def get_table(table_name: str) -> dataframe.DataFrame:
    """connection"""
    return connection.option("dbtable", f"{table_name}").load()


def select_1() -> dataframe.DataFrame:
    """SELECT 1"""

    return get_table('film_category')\
        .alias('a')\
        .join(get_table('category').alias('b'), F.col('a.category_id') == F.col('b.category_id')) \
        .groupBy("b.name")\
        .count()\
        .orderBy('count', ascending=False)


def select_2() -> dataframe.DataFrame:
    """SELECT 2"""

    return get_table('film_actor')\
        .alias('a') \
        .join(get_table('actor').alias('b'), F.col('a.actor_id') == F.col('b.actor_id')) \
        .join(get_table('inventory').groupBy("film_id").count().alias('c'), F.col('a.film_id') == F.col('c.film_id')) \
        .groupBy("b.first_name", "b.last_name")\
        .sum("c.count")\
        .orderBy('sum(count)', ascending=False)\
        .limit(10)


def select_3() -> dataframe.DataFrame:
    """SELECT 3"""

    return get_table('inventory')\
        .alias('a') \
        .join(get_table('film_category').alias('b'), F.col('a.film_id') == F.col('b.film_id')) \
        .join(get_table('category').alias('c'), F.col('b.category_id') == F.col('c.category_id')) \
        .join(get_table('film').alias('d'), F.col('a.film_id') == F.col('d.film_id')) \
        .groupBy("c.name")\
        .sum("d.rental_rate")\
        .orderBy('sum(rental_rate)', ascending=False)\
        .limit(1)


def select_4() -> dataframe.DataFrame:
    """SELECT 4"""

    return get_table('film')\
        .alias('a') \
        .join(
            get_table("film").select("film_id").
            exceptAll(
                get_table("inventory").select("film_id")
            ).alias('b'), F.col('a.film_id') == F.col('b.film_id')).select("title")


def select_5() -> dataframe.DataFrame:
    """SELECT 5"""
    actor_count = get_table("film_actor")\
        .alias("a")\
        .join(
            get_table("film_category")
            .where(F.col("category_id") == 3)
            .alias("b"), F.col('a.film_id') == F.col('b.film_id'))\
        .groupBy("a.actor_id")\
        .count()\
        .orderBy('count', ascending=False)

    df_unique_count = actor_count.groupBy("count")\
        .sum()\
        .select("count")\
        .orderBy('count', ascending=False)\
        .limit(3)

    return actor_count.alias("a")\
        .join(get_table("actor")
              .alias("b"),  F.col('a.actor_id') == F.col('b.actor_id'))\
        .join(df_unique_count.alias("c"), F.col('a.count') == F.col('c.count'))\
        .select("b.first_name", "b.last_name", "b.actor_id")


def select_6() -> dataframe.DataFrame:
    """SELECT 4"""
    city_active_status = get_table('customer')\
        .alias('a') \
        .join(get_table("address").alias('b'), F.col('a.address_id') == F.col('b.address_id'))\
        .join(get_table("city").alias('c'), F.col('b.city_id') == F.col('c.city_id'))\
        .groupBy("c.city")\
        .agg(
            F.sum(F.col("a.active")).alias('Active'),
            (F.count(F.col("a.active")) - F.sum(F.col("a.active"))).alias("Not active"))\
        .orderBy('Not active', ascending=False)  # Сгруппировал по не активным покупателям, тк их меньше

    return city_active_status


def select_7() -> dataframe.DataFrame:
    """SELECT 7"""

    start_with_a = get_table('rental')\
        .alias('a') \
        .join(get_table("customer").alias('b'), F.col('a.customer_id') == F.col('b.customer_id'))\
        .join(get_table("address").alias('c'), F.col('b.address_id') == F.col('c.address_id'))\
        .join(get_table("city").alias('d'), F.col('c.city_id') == F.col('d.city_id'))\
        .where(F.substring(F.col("d.city"), 1, 1) == 'a')\
        .groupBy("d.city")\
        .agg(F.sum(F.col("a.return_date")-F.col("a.rental_date")).alias("rental_sum"))\
        .select("d.city", "rental_sum")

    hyphen_in_line = get_table('rental')\
        .alias('a') \
        .join(get_table("customer").alias('b'), F.col('a.customer_id') == F.col('b.customer_id'))\
        .join(get_table("address").alias('c'), F.col('b.address_id') == F.col('c.address_id'))\
        .join(get_table("city").alias('d'), F.col('c.city_id') == F.col('d.city_id'))\
        .where(F.locate('-', F.col("d.city")) > 0)\
        .groupBy("d.city")\
        .agg(F.sum(F.col("a.return_date")-F.col("a.rental_date")).alias("rental_sum"))\
        .select("d.city", "rental_sum")

    return start_with_a.union(hyphen_in_line)


if __name__ == "__main__":
    select_1().show()
    select_2().show()
    select_3().show()
    select_4().show()
    select_5().show()
    select_6().show()
    select_7().show()
