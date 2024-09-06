# REGISTRY PRUNING

## Description

This is a DAG (Directed Acyclic Graph) that deletes untagged Docker images from the artifact registry. The DAG is designed to work with Google Cloud Platform's artifact registry. The DAG consists of two main tasks, pruning production repositories and pruning development repositories. The tasks are run on a weekly basis and they delete untagged Docker images in the specified repositories.


## DAG Configuration

### Default Arguments

The DAG is configured with the following default arguments:

  - **start_date**: September 20, 2022
  
  - **depends_on_past**: False
  
  - **retries**: 3


### Schedule

The DAG is scheduled to run 00:00 AM every Sunday.


### Parameters

The DAG is loaded with parameters from `config.yaml`.


## DAG Tasks

The DAG consists of two main tasks, pruning production repositories and pruning development repositories.

  - **start**: This is a `DummyOperator` task with the task_id *"start"*. It is the starting point of the DAG.
  
  - **pruning-prod_{repo}**: This task prunes the untagged Docker images from the production repositories. The task loops over the list of production repositories specified in the DAG parameters and deletes the untagged Docker images from each repository. It uses Google Cloud SDK's gcloud command and the `BashOperator` to accomplish this.
  
  - **pruning-dev_{repo}**: This task prunes the untagged Docker images from the development repositories. The task loops over the list of production repositories specified in the DAG parameters and deletes the untagged Docker images from each repository. It uses Google Cloud SDK's gcloud command and the `BashOperator` to accomplish this.
  
  - **end**: This is a `DummyOperator` task with the task_id *"end"*. It is the ending point of the DAG.


## Bash Command

### Description

The script run by the `BashOperator` is using the Google Cloud SDK's command-line tool gcloud to interact with the Artifact Registry service on Google Cloud Platform (GCP). The parameters are defined through the `bash_command` function. Here's a breakdown of each part of the command:

  - `gcloud artifacts docker images list`: This command lists all the Docker images in the specified repository.
  
  - `{LOCATION}-docker.pkg.dev/{PROJECT}/{REPOSITORY}`: This is the location of the repository in the Artifact Registry, where `{LOCATION}` is the region where the repository is located (e.g. `us-central1`), `{PROJECT}` is the ID of your GCP project, and `{REPOSITORY}` is the name of your repository.
  
  - `--filter='-tags:*'`: This flag filters the output of the list command to only show images that have no tags. The - sign in front of the tags field negates the filter, so it will return images that do not have any tags.
  
  - `--format="value(format("{0}@{1}",package, version))"`: This flag specifies the output format of the list command. In this case, it uses the value format to only show the image path and version number separated by an `@` symbol. For example: `my-image@sha256:abcdef0123456789`.
  
  - `--include-tags`: This flag includes tags in the output of the list command, even though we're filtering for images with no tags. This is important because we need to know which images to delete later.
  
  - ` | `: the pipe symbol (|) is used to redirect the output of one command to another command. It is called a pipe operator or a pipeline. When a command is piped, its output is used as the input to the next command in the pipeline.
  
  -  `xargs -I {image_path} gcloud artifacts docker images delete {image_path}`: This part of the command uses xargs to take the output from the list command and pass it as an argument to the gcloud artifacts docker images delete command, which deletes the specified image from the repository.