# BBC News Web Scraper & Visualizer (Airflow + MongoDB + D3.js)

This project was built as part of a technical assignment for DeepSearch Labs.

## Overview

The system scrapes BBC News articles daily for 5 days using Apache Airflow. It captures full article metadata including title, subtitle, text, topic, publication date, author, image links, and associated videos.

The collected data is cleaned, analyzed, and stored in MongoDB. A small API is used to serve this data to a frontend built with React and D3.js for visualization.

## Features

- 🔁 Automated daily scraping using Apache Airflow
- 📰 Article metadata extraction (menu, submenu, topic, title, subtitle, date, images, authors, video)
- 📊 Basic data analysis to determine content trends and distribution
- 💾 MongoDB integration using clean schema design
- 🖥️ Backend (Node.js/FastAPI) API to serve processed data
- 📈 D3.js + React frontend to display topic/category distribution

## Tech Stack

- Apache Airflow
- Python (for scraping & analysis)
- MongoDB
- React.js
- D3.js
- Node.js or FastAPI (for the API)
- Docker (for orchestration)
- Jupyter Notebook (for exploratory data analysis)

## Folder Structure

- `dags/` – Airflow pipelines for scraping
- `clean_data/` – Cleaned data files
- `processed_data/` – Final processed data ready for MongoDB
- `analysis_notebook.ipynb` – Exploratory data analysis & classification
- `webserver_config.py` – Webserver config for Airflow

## Setup

```bash
docker-compose up --build
