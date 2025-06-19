FROM public.ecr.aws/lambda/python:3.10

# Install Chromium and Chromedriver on Amazon Linux 2
RUN yum update -y \
    && yum install -y \
       chromium \
       chromedriver \
    && yum clean all \
    && rm -rf /var/cache/yum

# Set environment variables for Chromium
ENV CHROME_BIN=/usr/bin/chromium-browser \
    CHROMEDRIVER_BIN=/usr/bin/chromedriver

# Set working directory to Lambda task root
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy and install Python dependencies
COPY requirements.txt  .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy application code (including trigger.py, src/, config/, lambda_handler.py)
COPY . .

# Define the Lambda handler entry point
# Assumes you have created lambda_handler.py with a function named lambda_handler
CMD ["lambda_handler.lambda_handler"]
