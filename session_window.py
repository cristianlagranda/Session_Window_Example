import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window

service_account_path = os.environ.get("SERVICE_ACCOUNT_PATH")
print("Service account file : ", service_account_path)
input_subscription = os.environ.get("INPUT_SUBSCRIPTION")
output_topic = os.environ.get("OUTPUT_TOPIC")

options = PipelineOptions()
options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options=options)

def encode_byte_string(element):
    print (element)
    element = str(element)
    return element.encode('utf-8')

def calculateProfit(elements):
  buy_rate = elements[5]
  sell_price = elements[6]
  products_count = int(elements[4])
  profit = (int(sell_price) - int(buy_rate)) * products_count
  elements.append(str(profit))
  return elements

def print_element(element):
    print("intermediate result: ", element)
    return element

def decode_and_split(row):
    return row.decode('utf-8').split(',')

pubsub_data= (
                p
                | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= input_subscription)
                # STR_2,Mumbai,PR_265,Cosmetics,8,39,66/r/n
                | 'Remove extra chars' >> beam.Map(lambda data: (data.rstrip().lstrip()))  # b'STR_2,Mumbai,PR_265,Cosmetics,8,39,66'
                #| 'Print'>> beam.Map(print_element)
                #because we are receiving a byte string, instead of a string, we have to decode it before splitting it.
                | 'Split Row' >> beam.Map(decode_and_split)                             # [STR_2,Mumbai,PR_265,Cosmetics,8,39,66,]
                | 'Filter By Country' >> beam.Filter(lambda elements : (elements[1] == "Mumbai" or elements[1] == "Bangalore"))
                | 'Create Profit Column' >> beam.Map(calculateProfit)                              # [STR_2,Mumbai,PR_265,Cosmetics,8,39,66,27]
                | 'Form Key Value pair' >> beam.Map(lambda elements : (elements[3], int(elements[4])))  # STR_2 27
                | 'Window' >> beam.WindowInto(window.Sessions(25)) #After 25 seconds of inactivity for the key (category), it will give the result. The time is using is the timestamp that GooglePubSub uses when publishing
                | 'Sum values' >> beam.CombinePerKey(sum)
                | 'Encode to byte string' >> beam.Map(encode_byte_string)  #Pubsub takes data in form of byte strings
                | 'Write to pus sub' >> beam.io.WriteToPubSub(output_topic)
	             )

result = p.run()
result.wait_until_finish()
