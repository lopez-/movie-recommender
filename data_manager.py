import altair as alt
import streamlit as st
from pyspark.sql import functions as F

class DataManager(object):
    def __init__(self, movies, sim, pca):
        self.movies = movies.withColumnRenamed('movieId', 'id')
        self.sim = sim
        self.pca = pca

    def _as_pandas(self, df):
        return df.toPandas()
    
    def _throw_no_data_warning(self):
        st.warning('No data was found')

    def _throw_wrong_input_warning(self):
        st.warning('Wrong input')

    def write(self, thing):
        st.write(thing)
    
    def write_as_pandas(self, df):
        self.write(self._as_pandas(df))
    
    def write_movie_table(self, text):
        if text:
            s = f'.*{text.lower()}.*'.replace(' ', '.*')
            r = self.movies.filter(F.lower(self.movies.title).rlike(s))
        else:
            r = self.movies
    
        if r.count()==0:
            return self._throw_no_data_warning()

        self.write_as_pandas(r.select(['id', 'title']))
    
    def chart_neighbours_by_id(self, id, n_neighbours=10):
        if not id:
            return

        try:
            id = int(id)
        except:
            return self._throw_wrong_input_warning()
        
        selected_movie = self.pca.where(F.col('id')==id).withColumn('color', F.lit('selected'))
        sim_neighbours =  self.sim.where(F.col('id')==id).where(F.col('rank')<=n_neighbours).select('id2').withColumnRenamed('id2', 'id')
        neighbours = self.pca.join(sim_neighbours, on='id', how='inner').withColumn('color', F.lit('similar'))

        r = selected_movie.union(neighbours)

        r = r.join(self.movies, on='id', how='left')

        if r.count()==0:
            return self._throw_no_data_warning()

        c = alt.Chart(self._as_pandas(r)).mark_circle(size=500).encode(x='x', y='y', color='color', tooltip='title')
        t = c.mark_text(align='left', fontSize=12).encode(text='title')
        st.altair_chart(c+t, use_container_width=True)