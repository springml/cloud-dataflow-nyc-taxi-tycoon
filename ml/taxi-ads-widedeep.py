from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import time
import tensorflow as tf
 

CATEGORICAL_COLUMNS = ["ad_category", "taxi_dest_zone", "ad_event_period"]

COLUMNS = ['campaign_id', 'passenger_count', 'ad_category', 'taxi_dest_zone', 'ad_event_period']

def build_estimator(model_dir):
  """Build an estimator."""

  ad_category = tf.contrib.layers.sparse_column_with_hash_bucket(
    "ad_category", hash_bucket_size=100)
  taxi_dest_zone = tf.contrib.layers.sparse_column_with_hash_bucket(
    "taxi_dest_zone", hash_bucket_size=100)
  ad_event_period = tf.contrib.layers.sparse_column_with_hash_bucket(
    "ad_event_period", hash_bucket_size=100)

  # Continuous base columns.
  passenger_count = tf.contrib.layers.real_valued_column("passenger_count")

  # Transformations.
  passenger_count_buckets = tf.contrib.layers.bucketized_column(
    passenger_count, boundaries=[2, 5, 10])
  taxi_dest_zone_ad_category = tf.contrib.layers.crossed_column(
    [taxi_dest_zone, ad_category], hash_bucket_size=int(1e6))
  taxi_dest_zone_ad_event_period = tf.contrib.layers.crossed_column(
    [taxi_dest_zone, ad_event_period], hash_bucket_size=int(1e4))

  # Wide columns and deep columns.
  wide_columns = [passenger_count, passenger_count_buckets, 
    taxi_dest_zone_ad_category, taxi_dest_zone_ad_event_period]

  deep_columns = [
    tf.contrib.layers.embedding_column(ad_category, dimension=8),
    tf.contrib.layers.embedding_column(taxi_dest_zone, dimension=8),
    tf.contrib.layers.embedding_column(ad_event_period, dimension=8),
    passenger_count
  ]

  # m = tf.contrib.learn.LinearClassifier(
  #   model_dir=model_dir, feature_columns=wide_columns)

  #  m = tf.contrib.learn.DNNClassifier(
  #         model_dir=model_dir,
  #         feature_columns=deep_columns,
  #         hidden_units=[100, 50])

  m = tf.contrib.learn.DNNLinearCombinedClassifier(
    model_dir=model_dir,
    linear_feature_columns=wide_columns,
    dnn_feature_columns=deep_columns,
    n_classes=37,
    dnn_hidden_units=[100, 100, 100, 100])

  # # build 3 layer DNN multinomial with x units respectively.
  # z = tf.contrib.learn.DNNClassifier(feature_columns=deep_columns,
  #                                              hidden_units=[10, 20, 10],
  #                                              n_classes=37,
  #                                              model_dir=model_dir)
  return m
 
def generate_input_fn(filename):
  def _input_fn():
    BATCH_SIZE = 40
    filename_queue = tf.train.string_input_producer([filename])
    reader = tf.TextLineReader()
    key, value = reader.read_up_to(filename_queue, num_records=BATCH_SIZE)

    record_defaults = [[0],[0.0],[" "],[" "],[" "]]
    columns = tf.decode_csv(value, record_defaults=record_defaults)

    features, campaign_id = dict(zip(COLUMNS, columns)), columns[0]
    features.pop('campaign_id', 'campaign_id key not found')

    # works in 0.12 only
    for feature_name in CATEGORICAL_COLUMNS:
      features[feature_name] = tf.expand_dims(features[feature_name], -1)

    return features, campaign_id

  return _input_fn

def train_and_eval():
  test_file = "gs://ad-taxi/Taxi_Ad_Testing.csv"
  train_file = "gs://ad-taxi/Taxi_Ad_Training.csv"

  train_steps = 100

  model_dir = 'models/model_' + str(int(time.time()))
  print("model directory = %s" % model_dir)

  m = build_estimator(model_dir)
  print('estimator built')

  m.fit(input_fn=generate_input_fn(train_file), steps=train_steps)
  print('fit done')

  results = m.evaluate(input_fn=generate_input_fn(test_file), steps=1)
  print('evaluate done')

  print('Accuracy: %s' % results['accuracy'])

if __name__ == "__main__":
  print("TensorFlow version %s" % (tf.__version__))
  if tf.__version__ < '0.12.0':
    raise ValueError('This code requires tensorflow >= 0.12.0: See bug https://github.com/tensorflow/tensorflow/issues/5886')

  train_and_eval()