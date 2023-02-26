## Week 5 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the FHVHV 2021-06 data found here. [FHVHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz )


### Question 1:

**Install Spark and PySpark**

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?
- 3.3.2

> pyspark.__version__
> 3.3.2

</br></br>


### Question 2:

**HVFHW June 2021**

Read it with Spark using the same schema as we did in the lessons.</br>
We will use this dataset for all the remaining questions.</br>
Repartition it to 12 partitions and save it to parquet.</br>
What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.</br>


- 24MB

> 21.572226126988728


### Question 3:

**Count records**

How many taxi trips were there on June 15?</br></br>
Consider only trips that started on June 15.</br>

- 452,470

> spark.sql("""
SELECT COUNT(*)
FROM
fhvh_trips
WHERE
date(pickup_datetime) = '2021-06-15'
""").collect()


### Question 4:

**Longest trip for each day**

Now calculate the duration for each trip.</br>
How long was the longest trip in Hours?</br>

- 66.87 Hours

> [Row(duration=datetime.timedelta(days=2, seconds=67964))]
> days * 24 + seconds / 3600

### Question 5:

**User Interface**

 Spark’s User Interface which shows application's dashboard runs on which local port?</br>

- 4040

> https://spark.apache.org/docs/latest/monitoring.html#web-interfaces
> Every SparkContext launches a Web UI, by default on port 4040, that displays useful information about the application. This includes:

### Question 6:

**Most frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)</br>

Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone?</br>

- Crown Heights North


> df_zone_frequency = spark.sql("""
SELECT
    PULocationID,
    MAX(zones.Zone) as name,
    COUNT(*)
FROM
    fhvh_trips, zones
WHERE
    fhvh_trips.PULocationID == zones.LocationID
GROUP BY
    fhvh_trips.PULocationID
ORDER BY 3 DESC
""").collect()

> [Row(PULocationID=61, name='Crown Heights North', count(1)=231279),




## Submitting the solutions

* Form for submitting: https://forms.gle/EcSvDs6vp64gcGuD8
* You can submit your homework multiple times. In this case, only the last submission will be used.

Deadline: 06 March (Monday), 22:00 CET


## Solution

We will publish the solution here
