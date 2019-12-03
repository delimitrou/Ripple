import ripple

config = {
  "region": "us-west-2",
  "role": "serverless-role",
  "memory_size": 3008
}
pipeline = ripple.Pipeline(name="tide", table="s3://tide", log="s3://log", timeout=600, config=config)
input = pipeline.input(format="mzML")
step = input.split({"split_size": 100*1000*1000}, {"memory_size": 128})

params={
  "database_bucket": "database",
  "num_threads": 0,
  "species": "normalHuman",
}
step = input.run("tide", params=params, output_format="tsv")
step = step.combine(params={"sort": False}, config={"memory_size": 256})
params={
  "database_bucket": "database",
  "max_train": 10*1000,
  "output": "peptides",
}
step = step.run("percolator", params=params)
pipeline.compile("json/basic-tide.json", dry_run=False)
