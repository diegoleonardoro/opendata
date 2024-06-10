# opendata

This Python Flask application is designed to serve as a back-end for handling data queries related to city complaints and building permits. It integrates with MongoDB, Redis for caching, utilizes external APIs for data retrieval, and schedules tasks for data updates.

**Key Technologies:**
- Flask
- Redis
- APSchedule
- Requests
- Python Standard Libraries: json for data handling, datetime for date manipulations, gzip for data compression, and base64 for encoding binary data into ASCII characters.

**Flask Application Setup:**

**Routes:**

**311 Calls Route (/311calls):**

Fetches complaint data from Redis, decompresses it, and applies filters based on request parameters such need for a valid limit for pagination.
It handles data transformations to prepare summary counts and detailed data views based on user queries.

**311 Complaint Types Count Route (/311calls_complaint_types_count):**

Provides aggregated data suitable for charting purposes. It processes filtered data to prepare a time series count of complaints.

**DOB Approved Permits Route (/dob_approved_permits):** Similar to the 311 calls route but focuses on data regarding approved building permits.

**Data Compression and Decompression:**

Implements utility functions to compress data before caching and decompress when retrieving from cache. This approach minimizes memory usage and speeds up data transfer.
