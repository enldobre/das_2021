from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import *

host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
status_pattern = r'\s(\d{3})\s'
content_size_pattern = r'\s(\d+)$'


def extract_logs(dataframe):
    df = dataframe.select(regexp_extract('value', host_pattern, 1).alias('host'),
                     regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                     regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                     regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                     regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                     regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                     regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
    return df

