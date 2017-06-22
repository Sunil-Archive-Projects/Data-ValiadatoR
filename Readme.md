Data ValidatoR - A data Validation tool using R Language:

Features:

- Compare two large datasets with Millions of rows in matter of seconds
- Generate report for each run
- Maintian Logs of each run

Problem Statement:

Often, after ETL Process we need to check if the destination data has been modelled properly. To ensure this, we follow data validation process, which will implement the ETL logic into the data dump of both the DataSources which are available after ETL is complete. Reports and logs are generated to make sure that errors, if any, are caught.

Normally, the following fields will be out of sync between two datasets :

- Primary ID's
	-- Because the modelling logic will differ
- Dates
	-- The ETL may not consider Date and Time when converting
- Data Precision
	-- It is important to know if precision is correct for floats and doubles
- Mapping NULL values
	-- NULL values are handled differently by different processes. Consistency should be maintained


Benchmarks:

- Processes 220636 Rows with 47 columns of data (10.3 Million Cells of data) in Average of 1.52 seconds. [Best Case: 1.36 seconds, Worst Case: 2.20 seconds]

- Standardizes the above mentioned dataset for comparison [in 1.77 seconds]

- Finds duplicates in the Dataset and writes the duplicate records into a separate CSV [in 1.26 seconds]

- Compares 2 such input CSV dataset and creates a report which shows the difference for each cells for the corresponding claim_uid [in claims concept] and writes this report as CSV [in 2.27 seconds]

- Shows the count of differences, column wise, so that we know in which column, data comparison is failing the most

- Very Easy to Automate as it is just a ~10KB script , and needs a 1 line of command in batch file[Windows] or shell script [Linux] which can be scheduled using Windows Task Scheduler or a CRON job respectively

- Writes a log for each day’s run with Benchmarking parameters as well of the script run, to keep track of each run 