# Ripple

Ripple is a serverless computing framework designed to make complex applications on AWS Lambda.
Ripple allows the user to chain lambda functions using S3 triggers 

## Setup
Ripple works by chaining a series of Lambda functions together.
By default, the next function in the pipeline is triggered by S3.
However, for some functions such as `map` and `split`, a user specifies the next Lambda to invoke.

To set up an application pipeline, the user creates a json file such as the following:
```
{
  "input_name": "input-file.ext",
  "region": "us-west-2",
  "timeout": 300,
  "num_bins": 40,
  "bucket": "lambda-bucket",
  "setup": true,
  "functions": {
    "split-mzML": {
      "file": "split_file",
      "format": "mzML",
      "memory_size": 256
    },
    "tide": {
      "file": "application",
      "application": "tide",
      "memory_size": 1024,
      "database_bucket": "fasta"
    },
    "combine-tsv-files": {
      "file": "combine_files",
      "format": "tsv",
      "memory_size": 1024,
    }
  }
  "pipeline": [{
    "name": "split-mzML",
    "chunk_size": 100000,
    "ranges": false,
    "output_function": "tide"
  }, {
    "name": "tide",
    "num_threads": 0,
  }, {
    "name": "combine-tsv-files",
    "chunk_size": 1000000,
    "sort": false
  }]
}
```

Note that the user needs to create an S3 bucket for data and for logs.

The `functions` part is the set of all Lambda functions needed by the pipeline.
A function can be used multiple times in the pipeline.
This section specifies parameters that are applicable to every instance of the function, such as the amount of allocated memory or the file format.

The `pipeline` part specifies the order of execution of the Lambda functions.
The parameters in the section are for only instances that occur in this stage of the pipeline.
For example, you may want to call the same combine function twice in the pipeline, but only have one of them sort the output.
If the `output_function` is not specified, then the lambda function will write a file in the bucket specified by the `bucket` parameter.

The files written to S3 will have the format `<prefix>/<timestamp>-<nonce>/<bin_id>/<file_id>-<last>.<ext>.
If you want to use a file that is already on S3, add the key "sample_bucket" where the value is the name of the bucket. Then set "sample_input" to true.
Ripple will move the file from the sample bucket to the application bucket.

The prefix indicates the stage of the of the pipeline that wrote the file.
In the above example, output from the `tide` function would be prefixed with "2", while output from the `combine-tsv-file` would be prefixed with "3".
The timestamp indicates when a given run was instantiated and the nonce is an identifier for the run.
The bin ID specifies which bin the file is in. The bins are mostly in place for sorting purposes.
Each file in a bin is given a number between 1 and the number of files in the bin.
The `last` value indicates whether this is the maximum file ID associated with the bin. This is useful for combining files in bins.

## Functions
### Application
The application function allows a user to execute arbitrary code on the input file
##### Arguments
* application: The name of the application file in the `applications` directory to use.

### Combine
The combine function takes all files in a bin and concats the files into one file.
##### Arguments
* batch_size: Number of files to combine.
* chunk_size: Number of bytes to load at once.
* format: Type of file to be split (.mzML, .txt, .csv)
* identifier: Property to sort file values by.
* sort: True if the file values should be sorted.

### Initiate
The initiate function triggers a new lambda based on a prior step.
This is useful if there's a need to use data written by a Lambda other than the parent Lambda.
##### Arguments
* input_key_prefix: Prefix for the file we want to use.
* output_function: The lambda function to call for each bucket file.

### Map
The map function maps the input file to each file contained in a bucket.
##### Arguments
* bucket_key_value: The parameter the bucket file should represent in the next function.
* directories: True if the map function should send the directories in the map bucket, not the files.
* format: Type of file to be split (.mzML, .txt, .csv)
* input_key_value: The parameter the input file should represent in the next function.
* map_bucket: Bucket containing list of files to map.
* output_function: The lambda function to call for each bucket file.
* ranges: True if the input file represents pivots for sorting.

### Match
Give a set of keys, match returns the key with the highest score
##### Arguments
* chunk_size: Number of bytes to load at once.
* format: Type of file to be split (.mzML, .txt, .csv)
* find: What to look for. Currently only supports highest sum.
* identifier: Property to sort file values by.

### Pivot
Given a number of bins and an input file / file chunk, this function finds `num_bins` equally spaced pivots.
This is used for sorting input bins.
##### Arguments
* chunk_size: Number of bytes to load at once.
* identifier: Property to sort file values by.

### Sort
The sort function partial sorts the input by writing values to bins based on their sort value.
This function requires bin boundaries to be specified. This can be done using the `pivot_file` function.
##### Arguments
* chunk_size: Number of bytes to load at once.
* identifier: Property to sort file values by.

### Split
The split function sends offsets for a key to lambda functions so the file can be analyzed in parallel.
##### Arguments
* chunk_size: Number of bytes to load at once.
* format: Type of file to be split (.mzML, .txt, .csv)
* output_function: The lambda unction to call with each chunk.
* ranges: True if the input file represents pivots for sorting.

### Top
The top function extracts the items with the highest identifier value.
##### Arguments
* chunk_size: Number of bytes to load at once.
* format: Type of file to be split (.mzML, .txt, .csv
* identifier: Property to sort file values by.
* number: Number of values to return
