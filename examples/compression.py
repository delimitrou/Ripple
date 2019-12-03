import ripple

config = {
  "region": "us-east-1",
  "role": "serverless-role",
  "memory_size": 3008,
}
pipeline = ripple.Pipeline(name="compression", table="s3://compression", log="s3://log", timeout=600, config=config)
input = pipeline.input(format="bed")
step = input.sort(identifier="start_position", params={"split_size": 500*1000*1000}, config={"memory_size": 3008})
step = step.run("compress_methyl", params={"program_bucket": "program"})
pipeline.compile("json/compression.json")
