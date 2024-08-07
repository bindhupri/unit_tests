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
        "g1": 1.0,
        "pb1": 1.0,
        "qP": 1,
        "src": 1,
        "bvId": "155024682",
        "ptc": "PT1323014",
        "lpd": "20240707",
        "b1": 1.0,
        "g2": 1.0,
        "ipi1": 16.75,
        "g3": 1.0
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
        cid = f"{element['cid']}"
        prod_id_list = prod_id.split(',')
        items = self.generate_items(prod_id_list)
        data = {
            "pbs": 1,
            "ts": 1720602704,
            "id": str(cid),
            "ct": 3,
            "items": items
        }
        # Convert data to a compact JSON string
        yield data

# Define the pipeline options
pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    num_workers=5, 
    worker_machine_type='n1-standard-8',
    worker_disk_type='pd-ssd',
    worker_disk_size_gb=50,
    machine_type='n1-standard-8'
)
google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'sams-personalization-nba-dev'
google_cloud_options.job_name = 'parquet-to-json'
google_cloud_options.staging_location = 'gs://sams-personalization-nba-dev-export-bucket/harmony_poc/input_file_ds/staging'
google_cloud_options.temp_location = 'gs://sams-personalization-nba-dev-export-bucket/harmony_poc/input_file_ds/temp'
pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
pipeline_options.view_as(GoogleCloudOptions).region = 'us-central1'

# Configure worker options
#worker_options = pipeline_options.view_as(WorkerOptions)
#worker_options.num_workers = 2  # Start with a smaller number of workers
#worker_options.max_num_workers = 3  # Set the maximum number of workers

# Path to the input Parquet file in GCS
input_parquet_path = 'gs://sams-personalization-nba-dev-export-bucket/harmony_poc/input_file_ds/rye_reco_2024-06-22_part-000000000000_cid.parquet-00000-of-00001'

# Path to the output JSON file in GCS
output_json_path = 'gs://sams-personalization-nba-dev-export-bucket/harmony_poc/output_file/output.json'

# Create the pipeline
with beam.Pipeline(options=pipeline_options) as p:
    (
        p
        | 'Read from Parquet' >> ReadFromParquet(input_parquet_path)
        | 'Random Sample of One' >> Sample.FixedSizeGlobally(2)
        | 'Flatten List' >> beam.FlatMap(lambda x: x)  # Since Sample returns a list of lists
        | 'Process Elements' >> beam.ParDo(ProcessElement())
        | 'Write to File' >> WriteToText(output_json_path, num_shards=1, shard_name_template='', append_trailing_newlines=False)
    )
