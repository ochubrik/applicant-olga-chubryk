# Senior Data Engineer Tech Task

## Introduction
This technical challenge is designed to test your expertise in ETL, specifically using GCP services. 

As a Senior Data Engineer, your task is to design a production-grade, robust ETL pipeline to extract data from an open source API, set up the pipeline with daily loading, and perform some transformational analysis.

> [!NOTE]
> You are not required to spend more than 1.5 Hrs on this task. If you get stuck, simply proceed with another part of this task and include a README in your submission what steps you would have taken to complete the task.**

## Getting Started
1. Read through the whole README before starting.
2. Fork following this format `applicant-[yourGithubUsername]`, e.g. user `john-smith` should use `applicant-john-smith` and clone this repository. Placeholder directories have been created to get you started.
3. Follow the instructions outlined in the [Task Details](#task-details) section.
   1. [ETL Challenge](#etl-challenge) - Build an ETL pipeline
   2. [Analysis Challenge](#analysis-challenge) - Write SQL queries to answer the posed questions
   3. [Submit](#submit) - Grant us access to the project to evaluate your submission
4. Write unit tests to cover your code.

## Task Details

### Resources
- OpenWeather API (https://openweathermap.org/api) - sign up for a free to use API key.
- Google Cloud Platform (https://console.cloud.google.com/) - sign up with a new Google account for $300 in free credit (services free below a quota: https://cloud.google.com/free). 
> [!NOTE]
> This tech task only requires the free tier of these platforms. GCP will automatically end the trial in 90 days and cap your credits (https://cloud.google.com/signup-faqs).

> [!CAUTION]
> If you wish to continue using your credits after this tech task, please decommission any of the charging services and APIs used for this tech task to avoid excessive credit use.

### ETL Challenge

Use your personal judgement to identify appropriate GCP service(s) to construct this pipeline.

1. For *ALL* of the below US cities, load in the 1st weeks worth of data for 2024 from OpenWeather API (https://openweathermap.org/api). This will be used for analysis later.
   - Sioux Falls,US
   - Great Falls,US
   - Houghton,Michigan,US
   - Fargo,North Dakota,US
   - Duluth,Minnesota,US
   - Bismarck,North Dakota,US
   - Aberdeen,South Dakota,US
   - Grand Island,Nebraska,US
   - Glasgow,Montana,US
   - Omaha,US
   - Portland,US
2. Create a pipeline to load yesterday's data daily. As this uses GCP services that charges your credits, feel free to simply save your deploy script in this repo and an instructional after testing. You can then delete the service or disable the API.
3. In BigQuery, load the data into object(s) prefixed with your initials, e.g. John Smith will create object(s) prefixed `js_` 

### Analysis Challenge
Write SQL queries to answer the following questions use the publicly available dataset `bigquery-public-data.geo_us_boundaries` by matching with the OpenWeatherMap data, all of the questions pertain to the historical data load:
1. Provide the average daily temperature for each city in each state.
2. Find the top 3 cities with the highest average humidity in each state.
3. Find the percentage of cities in each state experiencing "rain" as the weather condition.

Make sure that your queries can be run against the database created in the ETL.

### Submit
Grant the following people `Editor` access to your GCP project, this is needed to bring your services into our environment which allows you to decommission your services if you wish.
- tommy.thai@essencemediacom.com
- charlie.cleaver@essencemediacom.com

## Deliverables
1. [ ] **Python ETL** - A production ETL pipeline that is ready to run and deployable.
2. [ ] **Unit Tests** - A collection of unit tests for your ETL pipeline.
3. [ ] **SQL** - SQL schema creation file and any additional SQL query files.
4. [ ] **SQL Answers** - Answers to the SQL analysis challenge questions in a Markdown file, including the SQL queries used.
5. [ ] **Evidence** - Any additional evidence not already supplied e.g. deploy scripts and resources for GCP services consuming credits.
6. [ ] **README** - A detailed README.md that includes all required documentation.
7. [ ] Any additional documentation you think would be helpful for understanding your approach.

## Evaluation Criteria
1. [ ] **Code Quality**: Efficient data manipulation with Python and SQL. Clear, well-structured code that clearly has maintainability and collaboration in mind.
2. [ ] **DB Proficiency**: Demonstrate SQL optimization and complexity through the analysis challenge. And demonstrating understanding of database optimisations with future proofing in mind.
3. [ ] **Error Handling**: Gracefully handle potential errors or irregularities in data and in process.
4. [ ] **Testing**: Include unit tests to verify each part of the ETL process.
5. [ ] **Documentation**: A clear README.md that explains setup, execution, any assumptions made, and any other information you would like to include.

## How To Submit Your Solution
Fork this repository following this format `applicant-[yourGithubUsername]`, e.g. user `john-smith` should use `applicant-john-smith`. Commit your changes to your repo, and send a pull request to the original repo. 

Grant the following people `Editor` access to your GCP project, this is needed to bring your services into our environment which allows you to decommission your services if you wish.
- tommy.thai@essencemediacom.com
- charlie.cleaver@essencemediacom.com

We will review your submission and get back to you with our feedback.

Good luck!
