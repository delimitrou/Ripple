PIPELINE TESTING

Ripple provides functionality for a user to write tests for the pipeline without interacting with Amazon.

To see an example, look at pipeline_ssw_test.py

In this example, we create dummy buckets and objects to represent the Amazon classes needed by Smith Waterman.

We store the actual data in a tests subfolder called ssw (This isn't part of the repo, as the testing data is too big).

Look at the line
```
pp.populate_table("ssw-program", "ssw/", files)
```

For every file in files, the pipeline class looks in the subfolder "ssw/" and populates the test table "ssw-program" with these files.

The line

```
pp.run(name, "ssw/input-10.fasta")
```

initiates the pipeline for using name as the key name and the content from ssw/input-10.fasta as the content for the object.

Once the pipeline is done, all intermediate and final results can be accessed using

```
entries = pp.database.get_entries(pp.table.name)
```
