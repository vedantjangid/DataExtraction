import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io import WriteToText
from apache_beam.transforms.core import Create
import argparse
import logging
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

class DataTransform(beam.DoFn):
    def process(self, element):
        # Implement your transformation logic here
        # For example, let's capitalize all string fields
        return [{k: v.upper() if isinstance(v, str) else v for k, v in element.items()}]

class DataIntegrityCheck(beam.DoFn):
    def __init__(self):
        self.row_count = 0

    def process(self, element):
        self.row_count += 1
        yield element

    def finish_bundle(self):
        logging.info(f"Total rows processed: {self.row_count}")
        yield beam.pvalue.TaggedOutput('row_count', self.row_count)

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', required=False, help='Start date for data extraction (YYYY-MM-DD)')
    parser.add_argument('--end_date', required=False, help='End date for data extraction (YYYY-MM-DD)')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    # Validate date inputs if provided
    date_filter = ""
    if known_args.start_date or known_args.end_date:
        try:
            if known_args.start_date:
                start_date = datetime.strptime(known_args.start_date, '%Y-%m-%d')
                date_filter += f"DATE(timestamp_column) >= '{known_args.start_date}'"
            
            if known_args.end_date:
                end_date = datetime.strptime(known_args.end_date, '%Y-%m-%d')
                date_filter += f" AND " if date_filter else ""
                date_filter += f"DATE(timestamp_column) <= '{known_args.end_date}'"
            
            if known_args.start_date and known_args.end_date and start_date > end_date:
                raise ValueError("Start date must be before end date")
        except ValueError as e:
            logging.error(f"Invalid date input: {str(e)}")
            return

    # Construct BigQuery query
    query = f"""
    SELECT *
    FROM `{os.getenv('PROJECT_ID')}.{os.getenv('DATASET_ID')}.{os.getenv('TABLE_ID')}`
    """
    if date_filter:
        query += f" WHERE {date_filter}"

    # Create and run the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        # Read from BigQuery
        data = p | 'Read from BigQuery' >> ReadFromBigQuery(query=query, use_standard_sql=True)

        # Apply transformation
        transformed_data = data | 'Transform Data' >> beam.ParDo(DataTransform())

        # Apply data integrity check
        checked_data = transformed_data | 'Data Integrity Check' >> beam.ParDo(DataIntegrityCheck()).with_outputs('row_count', main='data')

        # Write to CSV
        output_path = os.path.join(os.getenv('OUTPUT_BUCKET'), f'output_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
        (checked_data.data 
         | 'Convert to CSV' >> beam.Map(lambda row: ','.join(str(field) for field in row.values()))
         | 'Write to CSV' >> WriteToText(output_path, file_name_suffix='.csv'))

        # Log row count
        (checked_data.row_count 
         | 'Log Row Count' >> beam.Map(lambda count: logging.info(f"Final row count: {count}")))

        # Perform additional check: compare row counts
        expected_count = p | 'Create Expected Count' >> Create([None])  # You would replace this with actual expected count
        actual_count = checked_data.row_count

        def check_row_count(expected, actual):
            if expected is not None and expected != actual:
                logging.error(f"Row count mismatch. Expected: {expected}, Actual: {actual}")
            else:
                logging.info("Row count check passed")

        (expected_count, actual_count) | 'Check Row Count' >> beam.Map(check_row_count)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()