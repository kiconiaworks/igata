# igata README

This project provides a inference infrastructure for easily preparing and deploying a trained model and wrapping the model in a way that easily allows 
input/output to various services (for example: SQS, S3, DyanamoDB)

## Usage

1. Build and train your model,

2. Implement your model for prediction by sub-classing `igata.predictors.PredictorBase` and implementing the required methods. 

    -Available Methods:
        - preprocess_input(_input_record_, _meta_)
        - predict(_input_record_, _meta_) [*REQUIRED*]
        - postprocess_output(_prediction_result)

    > NOTE:
    > Currently igata only supports images (png|jpg) as inputs from S3, or SQS/S3
    > _input_record_ is provided to the `preprocess_input()` method as a *numpy.array*. 

## Execution

Once you have a Predictor class sub-classing `igata.predictors.PredictorBase`, prepare a DockerFile to build the combined image.

Execution is performed through the `igata.cli` entry point.
Environment Variables are used to control the input/output managers to use. (See sections below)
    
> The entry point for execution is through `igata.cli`    

Example:
```bash
PREDICTOR_MODULE=dummypredictor.predictors PREDICTOR_CLASS_NAME=DummyPredictorNoInputNoOutput OUTPUT_CTXMGR_SQS_QUEUE_URL=http://localhost:4576/queue/test-queue pipenv run python -m igata.cli s3://test-bucket/720503_273_2014960_tn.jpg
```

## Environment Variables

Environment variables are used to control the input/output for a given predictor.

The following environment variables can be used to control a built image executor.


### General Environment Variables

- `LOG_LEVEL` set output log level, DEBUG, INFO, WARNING
- `PREDICTOR_MODULE`: Dotted path to module containing *user-defined* Predictor class (Ex: 'mypackage.submodule')
-  `PREDICTOR_CLASS_NAME`: [DEFAULT="Predictor"] *User-defined* Predictor class name that subclasses `igata.predictors.PredictorBase` (Ex: "MyPredictor")

### Input Context Manager Environment Variables

- `INPUT_CONTEXT_MANAGER`

Available Input Context Manager(s):

- 'S3BucketImageInputCtxManager': [DEFAULT] Pulls IMAGE inputs from s3 bucket/key given a list of s3Uris (Ex: s3://bucket/my/key.png)
    - Required Option(s) Environment Variables: *None*

- 'SQSRecordS3InputImageCtxManager': 
    - Required Option(s) Environment Variables: 
        - `INPUT_CTXMANAGER_SQS_QUEUE_URL`: Queue Url form which to retrieve messages from

### Output Context Manager Environment Variables

- `OUTPUT_CONTEXT_MANAGER`: Defines the OutputCtxManager to use. (See 'Available Output Context Managers below)
    
- `RESULT_RECORD_CHUNK_SIZE`: Defines the number of records that are cached before being sent to the OutputCtxManager's `put_records()` method.

Available Output Context Manager(s):

- 'SQSRecordOutputCtxManager': [DEFAULT] Output Predictor *results* to an SQS Message Queue
    - Required Option(s) Environment Variables:
        - `OUTPUT_CTXMGR_SQS_QUEUE_URL`: (str) Url to the result output sqs queue        
    
    
- 'S3BucketCsvFileOutputCtxManager'
    - Required Option(s) Environment Variables:
        - `OUTPUT_CTXMGR_OUTPUT_S3_BUCKET`: (str) Bucket name of output bucket
        - `OUTPUT_CTXMGR_FIELDNAMES`: (str) comma separated list of values defining the header fieldnames (Ex: "header1,header2,header3"

- 'DynamodbOutputCtxManager'
    - Required Option(s) Environment Variables:
        - `RESULTS_ADDITIONAL_PARENT_FIELDS`: (str) comma separated fields to include from parent record to include in result
        - `RESULTS_SORTKEY_KEYNAME`: (str) The field name of the dynamodb RESULTS Table sort-key (required to output to the result to the dynamodb results table)
        - `REQUESTS_TABLE_HASHKEY_KEYNAME`: (str) field name of the dynamodb REQUESTS Table hash-key. 
        - `REQUESTS_TABLE_RESULTS_KEYNAME`: (str) field name that defines the JSON results field content
        - `OUTPUT_CTXMGR_REQUESTS_TABLENAME`: (str) Dynamodb REQUESTS Table name, 'state' field will be updated
        - `OUTPUT_CTXMGR_RESULTS_TABLENAME`: (str) Dynamodb RESULTS Table name.  Will be populated with flattened results of the model result dictionary

#### DynamodbOutputCtxManager Table(s) Structure

##### REQUESTS Table

| AttributeName | Type | Is HASHKEY | Is RANGEKEY | GSI HASH_KEY | GSI RANGEKEY |
|---------------|:----:|:----------:|:-----------:|:------------:|:------------:|
| request_id    | S    | ○          | ✖           | ○            | ✖            |
| collection_id | S    | ✖          | ✖           | ✖            | ✖            |
| state         | S    | ✖          | ✖           | ✖            | ○            |

> GSI projection_type = ALL

##### RESULTS Table

| AttributeName | Type | Is HASHKEY | Is RANGEKEY | GSI HASH_KEY | GSI RANGEKEY |
|---------------|:----:|:----------:|:-----------:|:------------:|:------------:|
| hashkey       | S    | ○          | ✖           | ✖            | ✖            |
| s3_uri        | S    | ✖          | ○           | ✖            | ✖            |
| collection_id | S    | ✖          | ✖           | ○            | ✖            |
| valid_number  | S    | ✖          | ✖           | ✖            | ○            |

> GSI projection_type = ALL


## Local Development

Python: 3.7

> Requires [pipenv](https://pipenv.readthedocs.io/en/latest/) for dependency management
> Install with `pip install pipenv --user`


### Install the local development environment

1. Setup `pre-commit` hooks (_black_, _isort_):

    ```bash
    # assumes pre-commit is installed on system via: `pip install pre-commit`
    pre-commit install
    ```

2. The following command installs project and development dependencies:

    ```
    pipenv install --dev
    ```

### Run code checks

To run linters:
```
# runs flake8, pydocstyle
make check
```

To run type checker:
```
make mypy
```

### Running tests

This project uses [pytest](https://docs.pytest.org/en/latest/contents.html) for running testcases.

> NOTE: localstack is used for local aws service tests

`.env` for local testing:
```
S3_ENDPOINT=http://localhost:4572
SQS_ENDPOINT=http://localhost:4576
SQS_OUTPUT_QUEUE_NAME=test-output-queue
SNS_ENDPOINT=http://localhost:4575
DYNAMODB_ENDPOINT=http://localhost:4569
LOG_LEVEL=DEBUG
SQS_VISIBILITYTIMEOUT_SECONDS_ON_EXCEPTION=0
```


Tests cases are written and placed in the `tests` directory.

To run the tests use the following command:
```
docker-compose up -d
pytest -v
```

> In addition the following `make` command is available:

```
make test-local
```

## CI/CD Required Environment Variables

The following are required for this project to be integrated with auto-deploy using the `github flow` branching strategy.

> With `github flow` master is the *release* branch and features are added through Pull-Requests (PRs)
> On merge to master the code will be deployed to the production environment.

```
S3_ENDPOINT=http://localhost:4572
SQS_ENDPOINT=http://localhost:4576
SQS_OUTPUT_QUEUE_NAME=test-output-queue
```