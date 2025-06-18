# Data-pipeline-Brewery Data Ingestion and Enrichment

**Project Summary**

- This project is a modular data ingestion and enrichment pipeline that collects data from the Open Brewery DB API and processes it through the Medallion Architecture (Bronze → Silver → Gold).
- Built in Python with PySpark, the pipeline enables users to extract raw data, clean and transform it, and output structured datasets for analytics.
- Users can run each stage independently or as a full pipeline using a Command-Line Interface (CLI), with optional support for Docker deployment.

**Techniques Used**

- **Programming:** Python 3.10+, PySpark, Click CLI, Requests
- **Architecture:** Medallion (Bronze/Silver/Gold)
- **Data Engineering:** API Data Ingestion, Data Cleaning, Partitioning, Enrichment
- **Logging & Testing:** Python logging module, pytest test suite, GitHub Actions
- **Deployment Options:** Virtual Environment or Docker Integration
- **Error Handling:** Retry logic with failed request tracking and structured logging
  
**Results and Impact**

- Enabled timestamp-based versioning for full traceability across ingestion stages.
- Reduced API failure rate by auto-retrying failed requests (default 5 times).
- Validated ingestion quality with record-level accuracy checks at each stage.
- Generated final Gold dataset that aggregates breweries by country and type for fast analytical queries.
- Provided clean and partitioned datasets, boosting scalability and performance in distributed environments.

**Conclusion**

- The Brewery Data Ingestion and Enrichment Tool provides a scalable, testable, and modular approach to building real-world data pipelines.
- It demonstrates strong practices in data engineering, including fault-tolerant ingestion, schema design, and transformation logic aligned with industry standards.  
- This project serves as a foundation for building more complex data integration systems while ensuring maintainability and clarity in data lineage.


