### Holiday regressors collection

This codes uses the `regressors_staging.holidays_info` table in BQ to create a regressor for each holiday and each year from observed visits data.

## TO RUN:
All of this code is currently run bty copying and pasting into BQ.
1. run `query_training_data.sql` in BQ this created the models in `regressors_staging.` dataset
2. run `transforming_holiday_regressors.sql` this creates interim tables and the final tables with take the form `forecast_output_{holiday}_transformed`
3. run `create_collections.sql` this created the final table in `regressors.holidays`


## Explaining the code
### Preparing historical data

Create training data for ARIMA model in bigquery. This is done in `query_training_data.sql`

1. For speed and costs efficiency this was done by first getting a standardised visits score from visits_observed table for each POI in the whole US for NAICS codes starting 44 and 45 (retail trade)

Then the median of visit_score across all the places was taken to give a single time series across all the historical dates

2. Train the model

Train an individual model for each regressor (holiday) and for each year. This is done using the `holidays_info` table which define the dates a holiday starts and ends.

Define difference between forecasted results (which show the seasonality and trend) and the actual observed visits score as the error.  This error becomes our regressor values for the holiday dates.

### Forecasting - `transforming_holiday_regressors.sql`
Transform the above errors so that they make sense within the larger picture e.g. shift so that they start and end at 0.

Forecast the regressors for future dates. e.g for 2021 takes the average values for 2019/2020.

Works fine if the holiday stays on the same date as just take 365 days previously.

If not we call this a ‘floating holiday’ e.g thanksgiving. Solved this by creating a function that finds the date of the holiday of a certain year if this holiday occurs at the same nth time of the month. e.g thanksgiving is always the 4th Thursday in November.
