import pandas as pd
import numpy as np
from uszipcode import SearchEngine
import sqlite3

search =  SearchEngine(db_file_dir="/tmp/db")
conn = sqlite3.connect("/tmp/db/simple_db.sqlite")
pdf = pd.read_sql_query("select  zipcode, lat, lng, radius_in_miles, 
                        bounds_west, bounds_east, bounds_north, bounds_south from 
                        simple_zipcode",conn)
brd_pdf = sc.broadcast(pdf)

                        
@udf('string')
def get_zip_b(lat, lng):
    pdf = brd_pdf.value
    try:
        out = pdf[(pdf['bounds_north']>=lat) & 
                  (pdf['bounds_south']<=lat) & 
                  (pdf['bounds_west']<=lng) &  
                  (pdf['bounds_east']>=lng) ]
        dist = [None]*len(out)
        for i in range(len(out)):
            dist[i] = (out['lat'].iloc[i]-lat)**2 + (out['lng'].iloc[i]-lng)**2
        zip = out['zipcode'].iloc[dist.index(min(dist))]
    except:
        zip = 'bad'
    return zip


output_df = df.withColumn('zip', get_zip_b(col("latitude"),col("longitude"))).cache()
