import dlt
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.extract.exceptions import ResourceExtractionError
from tenacity import (
    Retrying,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception,
)


@dlt.resource(name="example_resource")
def example_resource(should_fail_extraction: bool = False):
    """
    Resource that simulates data extraction failures when should_fail_extraction is True.
    It yields 10 items but raises an exception at the 5th item if should_fail_extraction is True.
    """
    for i in range(10):
        if should_fail_extraction and i == 5:
            raise Exception("Intentional failure at extraction step (i=5)")
        yield {"value": i}


# Define which exceptions should trigger a Tenacity retry
# returns true if the exception is an instant of ResourceExtractionError or PipelineStepFailed
# When this returns true, Tenacity will retry the pipeline run
# if it returns false, it means the exception is non-retriable
def _is_transient_dlt_exception(e: Exception) -> bool:
    """
    Decide which exceptions to treat as transient/retriable.

    We consider:
      - PipelineStepFailed (DLT's top-level pipeline error)
      - ResourceExtractionError (DLT's extraction error)
    """
    return isinstance(e, (ResourceExtractionError, PipelineStepFailed))


def run_pipeline_with_retry(pipeline: dlt.Pipeline, data):
    """
    Runs the DLT pipeline with retry logic, using Tenacity for transient failures.
    If the pipeline run fails after all retries, we re-raise the exception.
    """
    try:
        # Configure the retry strategy:
        # - Stop after 5 attempts
        # - Use exponential backoff waiting strategy
        # - Retry if the exception is a DLT extraction or pipeline-level error
        # - Reraise final exception if still failing after max attempts
        for attempt in Retrying(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception(_is_transient_dlt_exception),
            reraise=True,
        ):
            with attempt:
                print(f"Running pipeline, attempt #{attempt.retry_state.attempt_number}...")
                load_info = pipeline.run(data)
                print(f"Pipeline run completed successfully: {load_info}")
                # Slack notification
                print("Data was successfully loaded!")
    except Exception as e:
        # If we exhaust all retries, print error (Slack Notification)
        print(f"Pipeline failed after all retries: {str(e)}")
        raise

    # If we get here, at least one run was successful
    print("Pipeline finished successfully with a successful attempt.")
    return load_info


if __name__ == "__main__":
    # Create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="example_pipeline",
        destination="duckdb",
        dataset_name="example_data",
    )

    # Prepare the data: set should_fail_extraction to True so extraction fails at i=5
    data = example_resource(should_fail_extraction=True)

    # Run the pipeline with retry logic
    load_info = run_pipeline_with_retry(pipeline, data)

    # Post-load checks
    with pipeline.sql_client() as client:
        with client.execute_query("SELECT COUNT(*) FROM example_resource") as cursor:
            row_count = cursor.fetchone()[0]
            print(f"example_resource table row count: {row_count}")
