# Creation and Training of Models for Visits Scaling

[Lucidchart](https://lucid.app/lucidchart/a449f91f-bc48-4d83-96c4-dc5437737aca/edit)

## Table of Contents
1. [How to Use](#how-to-use)
2. [Known Issues](#known-issues)
3. [Usual Problems When Debugging](#usual-problems-when-debugging)
4. [Visits Share](#visits-share)
4. [Quality](#quality)
5. [Trend](#trend)
5. [Real Visits](#real-visits)

## How to Use
This pipeline is set to run every three months to keep up to date with the data. However, it can also be triggered to 
run manually if some changes are made to the models.

If only a part of the pipeline needs to be trained, the easiest way to do is to trigger a new run, mark all tasks,
as complete and then clear the task at the start of the part to be trained and let airflow clear all downstream tasks.
This way, all models that depend on the selected one will be retrained. The only issue with this approach is that some
tables that are supposed to be created previously in teh pipeline might be missing. In that case, it should be enough
to copy the missing tables from the production ones.

### VM Templates
Most of the models in this pipeline need to be trained in a VM at some point in the pipeline. In most of the cases, the
VM instance needs to be created before running. To make this process easier, there are two templates in the form of 
`machine images` already prepared in Compute Engine. One of them is intended to be used for training exclusively on CPU
(`cpu-python-template-prod`), and the other is prepared to train with a GPU (`tensorflow-template-prod`). Both of them
contain the entrypoint script `download_scripts.sh` already and are ready to use without further configuration.

The only model that does not make use of this template is quality. In this case, a persitent instance is used and it is 
simply turned on for training and of once it is done.

**Note:** template `tensorflow-template-prod` has not be tested in production.

## Known Issues
These issues do not break the pipeline immediately, but might cause problems down the road. In addition, they require 
extra manual interventions when changes are made to teh code that ideally could be automated.

### Table auto deletion
All tables created in this pipeline are saved in dedicated datasets with names prefixed by `smc_`. These are set up to 
add an expiration date of 30 days to any table created within. At the end of the pipeline, the largest tables are 
automatically deleted, but not every table is included in these tasks. Instead, it is expected the rest of the tables 
to expire after the 30-day period. However, there could be a conflict when running the pipeline again if the 
configuration of a table is changed, so it is still present when the pipeline is run again, or the pipeline is run less 
than 30 days after the end of th previous run.

### Script versions
Most scripts in this pipeline are copied from several sources, either other DAGs in this repository or some external 
repositories. When those sources are modified, changes have to be added manually to this pipeline. This introduces a 
risk that the scripts used in this repository will be out of date if the changes in the sources are not noticed. This 
also works the opposite way, as a bug might be found when working in this pipeline and never updated in the source.

### Parameters names
Different sources use different parameter names to refer to the same tables or locations. This pipeline had to settle in
one of those names to avoid having duplicated names for the same table or location. Therefore, parameter names have to 
be checked when copying code from a source.

### Run dates
When filtering by date in query, the macro `{{ ds }}` is used to determine up to which point the data needs to be 
selected. This works well when the pipeline is triggered manually, but will not work with a scheduled execution, being 
`{{ next_ds }}` the correct macro to get data up to the date the pipeline is run. This can probably be removed as 
we probably want all data available in the source tables.

### Run duration
During a run of the whole pipelines, some tables with inputs and outputs from the machine learning models need to be 
created in intermediate steps so thy can be sued later in the pipeline. This tables also need to be replaced in the 
production visits pipeline when the new models created with this pipeline replace the old ones. Ideally, the 
intermediate tables created while running this pipeline replaces the old tables without needing to compute them again.
However, this pipeline will most likely take more than one day to finish running, so some of these tables will be a few
days outdated by the time the pipeline finish. Therefore, there will be a gap in the data when these tables replace the
original ones. This is particularly relevant for the `poi_visits_scaled` table as it is a big table created with an 
expensive query.

Some possible solutions to this problem are:
- Manually fill in the missing days in teh final tables at the end of the pipeline.
- Add tasks to recompute all the tables at the end of the pipeline.
- Pause the daily update pipeline while this one is running and unpause once it is done.
- Automatically fill in the gaps at the end of the pipeline, using `BigQueryGetDataOperator` and XCOM features.


## Usual Problems When Debugging

- Wrong location of imports or queries files
- Parameter from other code is called different
- Old version of the code that has been changed somewhere else
- Github repository not updated or scripts not reloaded to VM


## Visits Share

### VM Configuration Selection
Controlled by parameter `visits_share_vm_template`: `cpu-python-template-prod` (CPU) or `tensorflow-template-prod` (GPU).

### Github
Alfonso needs to add people as collaborators:
[https://github.com/agarciadecorral/visits_share](https://github.com/agarciadecorral/visits_share)


## Quality

Scale the individual visits, so the total ratio visits/clusters during every day for one met area is smooth in time:

- All visits are included in the input, but some of them are not considered for the smoothing. The objective is that the
  model learns that it needs to assign a low value to these visits and the ones that might be similar but were not
  filtered:
    - Closed locations.
    - Locations near the home of the workplace of the device.
- The smoothing is done using a weighted moving average of 56 days to both sides.
- A simple weekly seasonality factor is included by subtracting the difference between the average for the current day
  of the week and the overall daily average from the smoothed value
- The training is done currently using met areas 0, 22, 33, 44, 66, and 77 for training and 11 for validation.


### VM Configuration Selection
Controlled by parameters:
- `quality_notebook_data_machine`: CPU configuration for data preprocessing.
- `quality_notebook_train_machine_config`: CPU configuration for training.
- `quality_notebook_train_accelerator_config`: GPU configuration for training.

### Github
[https://github.com/olvin-com/visits_quality](https://github.com/olvin-com/visits_quality)

### Execution details
The SSH command that trains the quality model might crash without the remote command failing due to the very long run 
time. Because of this, the training might succeed even if the train quality task fails. Therefore, if the command fails
after a long run time, the best solution is to manually check if the training is still going in the machine 
by checking the corresponding `tmux` session with `tmux a -t train`. If it is,
wait until is done, otherwise mark as succeeded the train task and keep going with the pipeline. This is also valid 
for a genuine error during training. Tensorflow has some random crashes when training, but after some time that is 
not a big issue as the latest checkpoint is good enough to be used.

With the current configuration, the training can be considered valid if it has gone on for more than 5 hours. It is 
recommended to run Tensorboard in the VM to check the convergence of the training process before proceeding.




## Trend

### VM Configuration Selection
Controlled by parameter `trend_vm_template`: `cpu-python-template-prod` (CPU) or `tensorflow-template-prod` (GPU).

### Github
[https://github.com/olvin-com/trend](https://github.com/olvin-com/trend)


## Real Visits

### VM Configuration Selection
Controlled by parameter `real_visits_vm_template`: `cpu-python-template-prod` (CPU) or `tensorflow-template-prod` (GPU).

### Github
[https://github.com/olvin-com/scaling_real_visits](https://github.com/olvin-com/scaling_real_visits)

## Future Work
- Create custom commonly used functions: 
[https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement)