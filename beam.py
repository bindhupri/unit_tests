import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions
from apache_beam.io import ReadFromParquet, WriteToText
from apache_beam.transforms.combiners import Sample
import json


class ProcessElement(beam.DoFn):
    default_values = {
        "ipi": 24.0,
        "lpq": 1.0,
        "tot_ord": 7,
        "id": "155024682",
        "g1": 0.15842,
        "pb1": 0.15842,
        "qP": 1,
        "src": 1,
        "bvId": "155024682",
        "ptc": "PT1323014",
        "lpd": "20240707",
        "b1": 0.15842,
        "g2": 0.15839,
        "ipi1": 16.75,
        "g3": 0.070629999
    }

    def generate_items(self, prod_id_list):
        items = []
        for prod_id in prod_id_list:
            item = {
                "ipi": self.default_values["ipi"],
                "lpq": self.default_values["lpq"],
                "tot_ord": self.default_values["tot_ord"],
                "id": self.default_values["id"],
                "g1": self.default_values["g1"],
                "pb1": self.default_values["pb1"],
                "qP": self.default_values["qP"],
                "p": prod_id,
                "src": self.default_values["src"],
                "bvId": self.default_values["bvId"],
                "ptc": self.default_values["ptc"],
                "lpd": self.default_values["lpd"],
                "b1": self.default_values["b1"],
                "g2": self.default_values["g2"],
                "ipi1": self.default_values["ipi1"],
                "g3": self.default_values["g3"]
            }
            items.append(item)
        return items

    def process(self, element):
        import random
        prod_id = element['prod_id']
        cid = random.randint(1, 1000000)
        prod_id_list = prod_id.split(',')
        items = self.generate_items(prod_id_list)
        result = {
            "CID": cid,
            "data": {
                "pbs": 1,
                "ts": 1720602704,
                "cid": str(cid),
                "ct": 3,
                "items": items
            }
        }
        yield json.dumps(result)

# Define the pipeline options
pipeline_options = PipelineOptions()
google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'playground-s-11-b8'
google_cloud_options.job_name = 'parquet-to-json'
google_cloud_options.staging_location = 'gs://shuvabuc002/staging'
google_cloud_options.temp_location = 'gs://shuvabuc002/temp'
google_cloud_options.region = 'us-central1'  # Ensure this is set correctly

# Configure worker options
worker_options = pipeline_options.view_as(WorkerOptions)
worker_options.num_workers = 2  # Start with a smaller number of workers
worker_options.max_num_workers = 3  # Set the maximum number of workers

# Path to the input Parquet file in GCS
input_parquet_path = 'gs://shuvabuc002/input/part-00000-c7585b4f-7e43-4685--c000.snappy.parquet'

# Path to the output JSON file in GCS
output_json_path = 'gs://shuvabuc002/output/output.json'

# Create the pipeline
with beam.Pipeline(options=pipeline_options) as p:
    (
        p
        | 'Read from Parquet' >> ReadFromParquet(input_parquet_path)
        | 'Random Sample of One' >> Sample.FixedSizeGlobally(1)
        | 'Flatten List' >> beam.FlatMap(lambda x: x)  # Since Sample returns a list of lists
        | 'Process Elements' >> beam.ParDo(ProcessElement())
        | 'Write to File' >> WriteToText(output_json_path, num_shards=1, shard_name_template='', append_trailing_newlines=False)
    )
