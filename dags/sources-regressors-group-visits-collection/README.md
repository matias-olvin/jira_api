README

## METHODOLOGY
This code creates regressor variables grouping historical data of poi visits scaled data by identifier.

### Historically
- create a group_visits table with the format of local_date | identifier | visit_score where identifier (at the moment is naics_code or s2_token). This is aggregated data from scaling poi visits table.
- Train an arima model to forecast for each identifier using the last 5 months of daily data form the group_visits table.

### Predictive
- Using the trained arima model to forecast future dates

### Daily running
- Every day update the group_visits table to append the newest available historical data overwriting the forecasted data.
- Every week (at the moment this is set to Monday) retrain the arima model and forecast then append this data to group_visits table.
