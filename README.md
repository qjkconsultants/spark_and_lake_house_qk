# STEDI Human Balance Analytics – Data Lakehouse Project

## Project Overview
This project showcases the design and implementation of a **cloud-based data lakehouse** to curate **sensor and mobile accelerometer data** for machine learning applications. Acting as a Data Engineer for the STEDI team, I built **end-to-end ETL pipelines** that process semi-structured JSON data, ensure data quality, and provide curated datasets for **real-time step detection models**.

The project demonstrates expertise in **AWS cloud services, PySpark, Python, and data engineering best practices**, with strong attention to **data privacy and governance**.

---

## Project Objectives
- Collect and process data from **Step Trainer motion sensors** and **mobile app accelerometers**.
- Create a **Landing → Trusted → Curated** data pipeline to enable **ML-ready datasets**.
- Sanitize and validate data to include **only customers who consented to research**.
- Join multi-source datasets to create a **machine learning curated table** aligning sensor readings with accelerometer data.
- Ensure **data privacy** compliance (GDPR/PII removal).

---

## Data Sources
- **Customer Data** – STEDI website and fulfillment records  
  Fields: `serialNumber`, `customerName`, `email`, `shareWithResearchAsOfDate`, `registrationDate`, etc.  
- **Step Trainer Sensor Data** – IoT device readings  
  Fields: `sensorReadingTime`, `serialNumber`, `distanceFromObject`  
- **Mobile App Accelerometer Data** – Device motion readings  
  Fields: `timeStamp`, `user`, `x`, `y`, `z`  

---

## Architecture & Workflow
1. **Landing Zone (S3)**  
   - Raw JSON ingestion into Glue tables: `customer_landing`, `accelerometer_landing`, `step_trainer_landing`  
   - Schema definition via **Glue Data Catalog**  
   - Initial row counts validated via **Athena SQL queries**

2. **Trusted Zone (S3 / Glue)**  
   - ETL jobs: `customer_landing_to_trusted.py`, `accelerometer_landing_to_trusted.py` implemented in **PySpark**  
   - Applied **filters to retain only consenting customers**  
   - Updated Glue table schemas dynamically with **Glue Studio**  
   - Validated row counts with **Athena**

3. **Curated Zone (S3 / Glue)**  
   - Created `customers_curated` by joining trusted customer and accelerometer tables  
   - Produced `step_trainer_trusted` and `machine_learning_curated` tables by joining sensor readings with curated customer data  
   - Ensured **timestamp alignment** for ML model readiness  
   - Removed PII for privacy compliance  

---

## Technologies & Tools
- **Cloud Services:** AWS Glue, AWS S3, AWS Athena  
- **Programming & ETL:** Python, PySpark, SQL, Glue Studio visual ETL  
- **Data Engineering Practices:**  
  - Landing → Trusted → Curated zones  
  - Dynamic schema evolution  
  - Data sanitization and validation  
  - Join and aggregation of multi-source datasets  
  - ML-ready data preparation  
- **Version Control:** GitHub for Python scripts, SQL scripts, and ETL workflows  

---

## Key Metrics & Achievements
| Zone    | Table                      | Row Count |
|---------|----------------------------|-----------|
| Landing | customer_landing           | 956       |
| Landing | accelerometer_landing      | 81,273    |
| Landing | step_trainer_landing       | 28,680    |
| Trusted | customer_trusted           | 482       |
| Trusted | accelerometer_trusted      | 40,981    |
| Trusted | step_trainer_trusted       | 14,460    |
| Curated | customers_curated          | 482       |
