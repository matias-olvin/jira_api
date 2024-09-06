## Visits Estimation Development
The aim of this DAG is helping to do the development of Visits Estimation Model.

Difficulties on how we validate or use the model (black box) make Airflow a useful tool.

### How to Use this DAG
1. Block 1 - Input Preparation
- Select a sample from poi_visits_scaled (optional)
- Processing like in Monthly Update (optional)
- Send Data to accessible_by_sns
    - `f"storage-dev-olvin-com.accessible_by_sns.visits_estimation_model_input_dev_{step}"`
2. Block 2 - Triggering the Model - See
- Trigger the SNS Pipeline. Once done mark as success
- Moving the data from accessible by olvin(and deleting) to the development dataset
    - `storage-dev-olvin-com.visits_estimation_model_dev`

3. Block 3 - Triggering the Validation Pipeline
- Moving the data to 
    - `f"storage-dev-olvin-com.accessible_by_sns.visits_estimation_model_dev_{step}"`
- Triggering the process
    - `sns-vendor-olvin-poc.accessible_by_olvin_dev.visits_estimation_model_dev_metrics_trend`
- Moving the data from accessible by olvin(and deleting) to the development metrics dataset
    - `storage-dev-olvin-com.visits_estimation_model_dev_metrics_trend`

It might seem there is too many movements sns->olvin->sns, but we have to thing of running the model as an isolated process, as a black box input output process.



## VALIDATION/MONITORING
https://lookerstudio.google.com/reporting/07196097-8f5b-41d6-a6bf-b1b261bfe89d/page/p_0a2b0omr3c