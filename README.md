# 🚕 NYC Taxi Trips 2024 – SQL Analysis  
I explored NYC Yellow Taxi Trips (2024) on DuckDB(SQL OLAP). I performed: data cleaning, validation, and analysis. Includes quality checks, 
seasonal trends, and revenue insights

## 🛠️ Tech Stack  
- **SQL** (DuckDB)  
- **VS Code** environment

### Why this project matters  
Taxi trip data reflects how people move, spend, and interact with urban transport. I demonstrate how SQL can uncover patterns in rider demand, seasonal 
revenue shifts, and data quality issues that affect decision-making. This project shows both **technical SQL skills** and the ability to translate raw 
data into **practical insights**.  

## 📜 Scripts  
🔗 [View the SQL Script](https://github.com/AtienoSandra/NYC-Yellow-2024-case-study/blob/main/nyc24_casestudy_queries.sql)  

## 📂 Workflow  
1. **Raw Data Load** – Import NYC taxi trips dataset.  
2. **Data Validation** – Apply rules for passenger count, distance, duration, and speed.  
3. **Flagging Records** – Mark trips as `Valid` or `Invalid` by validation checks.  
4. **Rejected Trips Table** – Isolate and summarize invalid trips for quality review.  
5. **Clean Dataset** – Keep only valid trips for analysis.  
6. **Analysis** – Explore seasonal trips, revenue, and driver efficiency metrics.  

## 🔑 Key Insights   

- **Data quality matters.** About **12% of all trips had to be rejected** due to odd values — like zero passengers, impossible distances, or trips that somehow lasted *negative minutes*. Cleaning was an essential first step before any real insights could surface.  

- **New Yorkers ride solo.** The average passenger count hovered around **1 per trip**, telling us that most yellow taxi rides are solo journeys — a reflection of the city’s fast, individual pace.  

- **The city moves in short bursts.** Typical rides covered **~3.6 miles in 17 minutes**, reminding us how taxis fill that sweet spot between walking and the subway.  

- **Revenue flows through credit cards.** On average, each trip brought in **$28.15**, with most riders swiping a card rather than paying cash.  

- **Seasons tell their own story.**  
  - **Fall** emerged as the **highest revenue season**, possibly fueled by tourism, events, and locals enjoying the city after summer heat.  
  - **Summer**, interestingly, had the **lowest revenue** — perhaps people preferred walking, biking, or using other ride-hailing services during warmer months.  

Each number paints a moving picture of how the city travels.  

## ▶️ How to Run  

This project is built using **DuckDB SQL**. Follow these steps to reproduce the analysis:  
**i.Clone the repo** 

**ii. Open DuckDB** - install DuckDB

**iii. Run the SQL script** - Launch DuckDB and execute the script

**iv.Check the output**





