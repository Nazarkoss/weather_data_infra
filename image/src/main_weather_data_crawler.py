import time
import json
import boto3
from io import BytesIO
import pandas as pd
import openmeteo_requests


_URL = "https://archive-api.open-meteo.com/v1/archive"
_FIELDS = ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation", "rain", 
           "snowfall", "snow_depth", "weather_code", "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low", 
           "cloud_cover_mid", "cloud_cover_high", "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", 
           "wind_speed_100m", "wind_direction_10m", "wind_direction_100m", "wind_gusts_10m", "soil_temperature_0_to_7cm", 
           "soil_temperature_7_to_28cm", "soil_temperature_28_to_100cm", "soil_temperature_100_to_255cm", "soil_moisture_0_to_7cm", 
           "soil_moisture_7_to_28cm", "soil_moisture_28_to_100cm", "soil_moisture_100_to_255cm", "is_day", "sunshine_duration"]

_CONFIG = {}
# Load external config
with open("stack_config.json") as json_file:
    _CONFIG = json.load(json_file)

s3 = boto3.client('s3')
bucket_name = _CONFIG["s3_bucket"]["bucket_name"]

def handler(event, context):    
    if event:
        batch_item_failures = []
        sqs_batch_response = {}
     
        for message in event["Records"]:
            try:
                print(message['body'])
                message_parsed = json.loads(message['body'])
                latitude = message_parsed["latitude"]
                longitude = message_parsed["longitude"]
                start_date = message_parsed["start_date"]
                end_date = message_parsed["end_date"]
                
                # Setup the Open-Meteo API client with cache and retry on error
                openmeteo = openmeteo_requests.Client()

                print("Fetching the data")

                # Make sure all required weather variables are listed here
                # The order of variables in hourly or daily is important to assign them correctly below
                params = message_parsed | {"hourly": _FIELDS}
                responses = openmeteo.weather_api(_URL, params=params)

                # Process first location. Add a for-loop for multiple locations or weather models
                response = responses[0]

                # Process hourly data. The order of variables needs to be the same as requested.
                hourly = response.Hourly()
                
                hourly_data = {"date": pd.date_range(
                    start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
                    end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
                    freq = pd.Timedelta(seconds = hourly.Interval()),
                    inclusive = "left"
                )}

                for i, field in enumerate(_FIELDS):
                    hourly_data[field] = hourly.Variables(i).ValuesAsNumpy()

                hourly_dataframe = pd.DataFrame(data = hourly_data)
                # Convert to datetime64[ms] to avoid unsupported format issues
                hourly_dataframe['date'] = hourly_dataframe['date'].dt.tz_localize(None).astype('datetime64[ms]')
                hourly_dataframe["latitude"] = latitude
                hourly_dataframe["longitude"] = longitude

                print("Data has been fetched for: {message_parse}")
                
                print("Writing to --{bucket_name}-- bucket")
                # Convert DataFrame to Parquet in-memory
                buffer = BytesIO()
                # Save the DataFrame to the buffer in Parquet format
                hourly_dataframe.to_parquet(buffer)

                # Upload to S3
                s3.put_object(Bucket=bucket_name, Key=f"raw/data-around-france/weather_lat_{latitude}_lon_{longitude}_{start_date}-{end_date}.parquet", Body=buffer.getvalue())
                print("Finished to push data to --{bucket_name}-- bucket")
                
                # Sleep between calls to avoid overheat the Openmeteo API
                time.sleep(3)

            except Exception as e:
                batch_item_failures.append({"itemIdentifier": message['messageId']})
        
        if batch_item_failures:
            print(f"FAILURES: {batch_item_failures}")
        sqs_batch_response["batchItemFailures"] = batch_item_failures
        return sqs_batch_response
    
    
