FROM public.ecr.aws/lambda/python:3.11

# Copy requirements.txt
COPY image/requirements.txt ${LAMBDA_TASK_ROOT}
COPY stack_config.json ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install -r requirements.txt

# Copy all files in ./src
COPY image/src/* ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler.
CMD [ "main_weather_data_crawler.handler" ]