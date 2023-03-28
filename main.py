#  Copyright 2023 Devoteam G Cloud
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

import apache_beam as beam
import pandas as pd
import json
import argparse
import datetime
from google.cloud import storage
from google.cloud import bigquery
from apache_beam.options.pipeline_options import PipelineOptions

OUT_TABLE_ID = "testing_table_pubsub"
OUT_DATASET_ID = "testing_data"
PROJECT_ID = "jordi-playground-data"
IN_BUCKET_ID = "ghack-data-challenge-2023"


def get_insert_date(ride, currentDateTime):
    """Loads current date into said field"""

    ride["INSERT_DATE"] = currentDateTime
    return ride


def write_to_bigquery(dataframe):
    """
    Creates a connection to a BigQuery project,
    creates a dataset and table object,
    sets the job configuration to `autodetect`, and then loads
    a dataframe into the BigQuery table.
    If any errors are encountered, they are printed.

    :param dataframe:
    :return:
    """
    client = bigquery.Client(project=PROJECT_ID)
    dataset = client.dataset(OUT_DATASET_ID)
    table_ref = dataset.table(OUT_TABLE_ID)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    errors = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)

    if errors:
        print(errors)


class ExtractObjectId(beam.DoFn):
    """
    Extracts the `objectId` from the
    `element.attributes` and returns it as a list.

    """
    def process(self, element):
        attribute_value = element.attributes['objectId']
        return [attribute_value]


def read_from_gcs(filename):
    """
    This code snippet is used to read data stored in a Google Cloud Storage (GCS) bucket.

    It begins by creating a storage client object and passing the project_id as an argument.
    Then, it gets the GCS bucket using the bucket id provided in the `IN_BUCKET_ID` variable.
    Finally, the code uses the `blob` method to download the file located in the GCS bucket
    with the name given as the `filename` parameter and returns the data as a string

    :param filename:
    :return:
    """
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.get_bucket(IN_BUCKET_ID)
    blob = bucket.blob(filename)
    return blob.download_as_string()


def dataframe_work(string):
    """
    Reads json data from a string, converts it into a `pd.DataFrame` object and return it as a list.

    First the string is decoded with `utf-8` encoding. Then each line is split using `\n` character as delimiter.
    The `line.strip()` ensures that any whitespace in the line is removed before being appended to the list.

    Then `json.loads()` is used to convert the json data from each line into a dictionary format.
    Finally, the `from_dict()` method is used to convert the dictionaries into a `pd.DataFrame` object and returned as a list.

    :param string:
    :return:
    """
    data = [json.loads(line) for line in string.decode('utf-8').split('\n') if line.strip()]
    df = pd.DataFrame.from_dict(data)
    return [df]


def main():
    """
    Main function that loads arguments and calls the run function

    :return:
    """

    parser = argparse.ArgumentParser(description="Data loading pipeline")
    parser.add_argument("--topic", help="Topic from PubSub")

    our_args, beam_args = parser.parse_known_args()
    run_pipeline(our_args, beam_args)


def run_pipeline(custom_args, beam_args):
    """
    Function that contains the big picture of the pipeline and every step of its execution

    :param custom_args:
    :param beam_args:
    :return:
    """
    current_date_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    opts = PipelineOptions(beam_args)

    with beam.Pipeline(options=opts) as p:
        files_to_load = (
                    p | 'Querying PubSub Topic' >> beam.io.ReadFromPubSub(topic=custom_args.topic, with_attributes=True)
                    | "Extract Attribute" >> beam.ParDo(ExtractObjectId()))

        load_rows_from_files = files_to_load | 'Reading files' >> beam.Map(read_from_gcs)

        records_to_dataframe = load_rows_from_files | 'Print records' >> beam.ParDo(dataframe_work)

        dataframe_to_bigquery = records_to_dataframe | 'Print records 2' >> beam.ParDo(write_to_bigquery)


if __name__ == '__main__':
    main()
