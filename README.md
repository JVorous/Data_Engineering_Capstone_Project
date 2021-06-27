<h2>Data Engineering Capstone Project</h2>

**REQUIREMENTS:**
<ol>
    <li>
        EMR Cluster for Spark
    </li>
    <li>
        S3 bucket holding data sets for input
    </li>
    <li>
        S3 bucket for outputting parquet files
    </li>
    <li>
        A configured dl.cfg file with correct values for the config parser.
    </li>
    <li>
        pyspark
    </li>
    <li>
        configparser
    </li>
    <li>
        os
    </li>
</ol>

**Project Summary**

This project aims to be able to answer questions on crimes in New York City, New York such as:
<ol>
    <li>
        What differences in crime exist between the 5 boroughs?
    </li>
    <li>
        What is the crime-per-population count in the 5 boroughs?
    </li>
    <li>
        Is there a correlation between rate of crime and the temperature?
    </li>
</ol> 

**Explanation**
Spark is chosen for this project as it is known for processing large amount of data fast 
(with in-memory compute), scales easily with additional worker nodes, has the ability to digest 
different data formats (e.g. Parquet, CSV, JSON), and integrates nicely with cloud storage like 
S3 and warehouse like Redshift.

There are also considerations in terms of scaling existing solution.

**If the data was increased by 100x:**

We could spin up larger instances of EC2s hosting Spark and/or additional Spark work nodes. 
With added capacity arising from either vertical scaling or horizontal scaling, we should be able to accelerate 
processing time.

**If the data populates a dashboard that must be updated on a daily basis by 7am every day:**

In this instance the pipeline could be shifted to Airflow to schedule and automate the data pipeline jobs. 
Built-in retry and monitoring mechanism can enable us to meet user requirement.

**If the database needed to be accessed by 100+ people:**

We could host the solution in a cloud data warehouse, with larger capacity to serve more users, and workload
management to ensure equitable usage of resources across users.

**Data sources**

This project sources the following data: 

<ul>
    <li>
        **NYPD Complaint Data**: Comes from a <a href="https://www.kaggle.com/adamschroeder/crimes-new-york-city"> 
Kaggle Data set</a> and describes crimes reported the various NYC burrows from 2013 - 2015.
    </li>
    <li>
        **World Temperature Data**: comes from <a href="https://www.kaggle.com/selfishgene/historical-hourly-weather-data"> 
Kaggle </a> and contains average weather temperatures hourly by city in Kelvin from 2013 - 2017.
    </li>
    <li>
        **NYC Borough Population**: Provided by 
<a href="https://data.cityofnewyork.us/City-Government/New-York-City-Population-by-Borough-1950-2040/xywu-7bv9">
NYC Open Data</a>. Lists the population of each of the 5 boroughs from 1950 through projected 2040. This is calculated
every 10 years, so to fit with the rest of the data set timeframe the population count from 2010 will be used.
    </li>
</ul>


This project uses Spark for ETL jobs and store the results in parquet for downstream analysis.

**Data Model**

<u>Dimension Tables</u>:

    crime_categories_table:
        crime_index
        crime_cat
        crime_desc

    boroughs_table:
        borough_index
        borough
        pop_count
        
    temperature_table:
        temp_index
        temp_date
        kelvin
        celsius
        fahrenheit

    date_table:
        date_index
        date
        dayofweek
        month
        year

<u>Fact</u>:

    crime_stats_df:
        id
        date_index
        borough_index
        temp_index
        crime_index
