"""Python Dataflow pipeline to demonstrate use of using
tabulate package, which does not come pre-installed on 
Dataflow worker VMs.
"""

import argparse
import logging
import re

from my_module import module_args

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from tabulate import tabulate



def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
    #   default='gs://mayo-dataflow-test/input.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
    #   default='tabulate',
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
#   pipeline_args.extend([
#       '--runner=DataflowRunner',
#       '--project=mayo-pso-pocs',
#       '--region=us-central1',
#       '--staging_location=gs://mayo-dataflow-test/stage',
#       '--temp_location=gs://mayo-dataflow-test/temp',
#       '--experiment=use_runner_v2',
#       '--jobName=tabulate-job',
#       '--worker_harness_container_image=gcr.io/mayo-pso-pocs/beam-python-sdk-gcp:3.0',
#       '--setup_file=./setup.py'
#   ])

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  with beam.Pipeline(options=pipeline_options) as p:
    
    my_module_args = module_args.ModuleArgs()
    # Read the text file into a PCollection.
    lines = p | ReadFromText(known_args.input)
    
    # Parse line to create YAML structure
    def pretty_print(line):
        words = line.split()
        return tabulate([words])

    output = lines | 'Format' >> beam.Map(pretty_print)

    # Write the output using a "Write" transform
    output | WriteToText(known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
