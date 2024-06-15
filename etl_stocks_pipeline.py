#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A word-counting workflow."""

# pytype: skip-file

# beam-playground:
#   name: WordCount
#   description: An example that counts words in Shakespeare's works.
#   multifile: false
#   pipeline_options: --output output.txt
#   context_line: 87
#   categories:
#     - Combiners
#     - Options
#     - Quickstart
#   complexity: MEDIUM
#   tags:
#     - options
#     - count
#     - combine
#     - strings

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import yfinance as yf
import pandas as pd
import ta


START_DATE = '2019-01-01'
END_DATE = '2023-12-01'
TICKERS = ['AAPL','PYPL','MSFT']

class FetchStockData(beam.DoFn):
  def process(self, ticker):
    data = yf.download(ticker, start=START_DATE, end=END_DATE)
    data.columns = data.columns.str.lower()
    data = data.reset_index()
    data['ticker'] = ticker
    new_columns = ['ticker'] + [c for c in data.columns if c != 'ticker']
    data = data[new_columns]
    # data = ta.add_all_ta_features(data, open='open', high='high', low='low', close='close', volume='volume')
    # data.fillna(0, inplace=True)
    yield data

class FlattenList(beam.DoFn):
    def process(self, elements):
        for element in elements:
            yield element

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    tickers = p | 'CreateTickerList' >> beam.Create(TICKERS)

    processed_data = (
        tickers
        | 'FetchStockData' >> beam.ParDo(FetchStockData())
        | 'CombineData' >> beam.CombineGlobally(beam.combiners.ToListCombineFn())
    )     

    # Convert DataFrames to CSV format
    csv_lines = (
        processed_data        
        | 'MergeDataFrames' >> beam.Map(lambda dfs: pd.concat(dfs).to_csv(index=False))
    )

    # Write results to csv file
    csv_lines | 'WriteToCSV' >> beam.io.WriteToText(known_args.output, file_name_suffix='.csv')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
