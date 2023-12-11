# Coding Challenge
This is my implementation of a data pipeline to load and transform the windfarm data.

The `main.py` entry point reads the CSVs, transforms the data and then saves it to two delta tables as might be found in a data lake / data platform. It uses functions defined in `utils.py` which are designed to be modular and testable.

The fact that the input CSVs are appended to each day is not ideal as it means we must scan the whole file each time to retrieve the latest changes. I still decided to go for an incremental load approach however, to avoid cleaning and calculating summary statistics for the whole file each day, which would not be scalable as the data grew over time.

Assumptions made:
* I impute missing/outlier values if a row is present and has some valid values in other columns, but not if a whole row for a given hour is missing.
* I chose an imputation approach based on a narrow window of one row before and after the missing data. If long sequences of missing data turned out to be common we could use a different approach, such as a wider window or imputing average values from the whole day.
* I understood "turbines whose output is outside of 2 standard deviations from the mean" to mean a turbine whose mean for the day was two standard deviations outside the mean for all turbines for that day.
* Output tables are delta tables 