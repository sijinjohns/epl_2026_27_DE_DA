# ⚽ EPL 2026/27 — Data Engineering & Analytics Project

A complete **Data Engineering + Analytics project** built around the English Premier League 2026/27 season, using real-world football data from multiple API sources.

The project demonstrates an end-to-end **ELT pipeline**, from ingesting raw API responses to transforming the data using Databricks and building an interactive analytics solution in Power BI.

## 🚀 Project Overview

The objective of this project was to build a scalable football analytics platform that can ingest data from multiple real-time API sources, process both structured and semi-structured JSON data, transform it through a **Medallion Architecture**, and make it available for business analysis through Power BI.

### 🔄 End-to-End Architecture

**API Sources → Raw JSON → Databricks → Bronze → Silver → Gold → Power BI**

The project follows an **ELT approach**, where raw data is first extracted and loaded into the data platform before transformation.

## 🛠️ Technologies Used

* **Databricks**
* **Apache Spark / PySpark**
* **Spark Declarative Pipelines**
* **Delta Lake**
* **Auto Loader**
* **Auto CDC**
* **Python**
* **REST APIs**
* **Power BI**
* **DAX**

## 📥 Data Ingestion

Data is collected from multiple real-world football API sources, including:

* EPL match data
* EPL team data
* Fantasy Premier League player data

The API responses are semi-structured and contain nested JSON objects and arrays.

Instead of immediately transforming the API response, the raw responses are preserved as **JSON files** in the Databricks data platform.

This provides a raw source layer that can be reprocessed whenever required.

## 🏗️ Medallion Architecture

The data is processed using a **Medallion Architecture** implemented with Databricks Spark Declarative Pipelines.

### 🥉 Bronze Layer

The Bronze layer ingests raw JSON data using **Auto Loader**.

Responsibilities include:

* Incremental file ingestion
* Preserving source data
* Handling schema evolution
* Capturing ingestion metadata

### 🥈 Silver Layer

The Silver layer is responsible for cleaning, validating, and transforming the raw data into reliable analytical entities.

Operations include:

* Data cleansing
* Data validation
* Duplicate handling
* Data type transformation
* Business rule validation
* Change Data Capture using **Auto CDC**
* Quarantining invalid records

### 🥇 Gold Layer

The Gold layer contains business-ready datasets designed specifically for analytics and reporting.

Examples include:

* Teams
* Players
* Matches

The Gold layer provides a clean and simplified data model for Power BI.

## 📊 Power BI Analytics

The processed Gold data is connected to Power BI, where I created the data model, relationships, calculated measures, and DAX logic required for the analytics layer.

The dashboard consists of **three main pages**.

### 1️⃣ Overall — League Overview

Provides an overall view of the EPL season.

Includes:

* League KPIs
* Overall league standings
* Wins
* Draws
* Losses
* Goals scored
* Goals conceded
* Goal difference
* Points
* Match and league performance insights

### 2️⃣ Team Analysis

Allows users to select a team and analyse its performance.

Includes:

* Team performance
* Match-by-match form
* Wins / Draws / Losses
* Goals scored and conceded
* Home vs Away performance
* Team players
* Player statistics within the selected team

### 3️⃣ Player Analysis

Provides detailed player-level analysis.

Includes:

* Top goal scorers
* Top assist providers
* Expected Goals (xG)
* Expected Assists (xA)
* Player points
* Minutes played
* Player performance statistics
* Player status and other relevant metrics

## 🧠 Key Data Engineering Concepts Demonstrated

This project helped me work with several real-world data engineering concepts:

* **ELT architecture**
* **REST API ingestion**
* **Semi-structured JSON processing**
* **Incremental data ingestion**
* **Auto Loader**
* **Delta Lake**
* **Spark Declarative Pipelines**
* **Medallion Architecture**
* **Data cleansing and validation**
* **Change Data Capture**
* **Auto CDC**
* **Data quality and quarantine**
* **Data modelling**
* **Power BI**
* **DAX**
* **Business-focused analytics**

## 🎯 Project Outcome

This project demonstrates an end-to-end data platform that takes raw football API data and transforms it into meaningful analytical insights.

The complete flow is:

```text
Real-World APIs
      ↓
Raw JSON Files
      ↓
Databricks
      ↓
Bronze Layer
      ↓
Silver Layer
      ↓
Gold Layer
      ↓
Power BI Data Model
      ↓
DAX & Analytics
      ↓
Interactive EPL Dashboard
```

The project was built with a focus on **real-world data engineering practices rather than simply creating a dashboard from an existing dataset**.
