import dlt

@dlt.resource(name='failing_resource')
def failing_resource(should_fail=True):
    for i in range(10):
        if should_fail and i == 5:
            raise Exception("Intentional failure at i=5")
        yield {'value': i}

def run_pipeline(should_fail=True):
    pipeline = dlt.pipeline(
        pipeline_name='test_pipeline',
        destination='duckdb',
        dataset_name='test_dataset',
        #dev_mode=False
    )

    try:
        info = pipeline.run(failing_resource(should_fail=should_fail),write_disposition='append')
        print(f"INFO: {info}")
        print(f"Pipeline run completed successfully with should_fail={should_fail}")
    except Exception as e:
        print(f"EXCEPTION: {e}")


if __name__ == "__main__":
    print("Intentional Failure")
    run_pipeline(should_fail=True)

    # print("Successful Completion")
    # run_pipeline(should_fail=False)
