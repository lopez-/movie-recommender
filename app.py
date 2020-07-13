from data_manager import DataManager
import os
from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
import streamlit as st

SPARK_PACKAGES = "io.delta:delta-core_2.12:0.7.0,org.apache.hadoop:hadoop-aws:2.7.3"
DELTA_CONF = "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages {0} --conf {1} --conf spark.driver.memory=10g pyspark-shell".format(SPARK_PACKAGES, DELTA_CONF)
)

spark_session = SparkSession.builder.getOrCreate()
sc = spark_session.sparkContext
spark = SQLContext(sc)

# read in data
movies = spark.read.format('delta').load('movies')
sim = spark.read.format('delta').load('similarity_matrix')
pca = spark.read.format('delta').load('pca')

dm = DataManager(movies=movies, sim=sim, pca=pca)

def main():
    st.title('IMDb Movie Recommender')

    spark_url = spark_session.sparkContext.uiWebUrl

    st.write(f'Spark URL: {spark_url}')

    movie_text_input = st.text_input(label='Search movie:')
    dm.write_movie_table(text=movie_text_input)
        
    if movie_text_input:
        movie_id_input = st.text_input(label='Search movie (ID):')
        dm.chart_neighbours_by_id(id=movie_id_input)

if __name__ == "__main__":
    main()