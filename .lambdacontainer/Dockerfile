# 1️⃣ Partimos de Amazon Linux 2 puro
FROM amazonlinux:2

# 2️⃣ Instalar Python3, pip y el Lambda Runtime Interface Client
RUN yum install -y python3 python3-pip zip \
    && pip3 install --upgrade pip awslambdaric \
    && yum clean all

# 3️⃣ Habilitar EPEL para Chromium
RUN yum install -y epel-release \
    && yum clean metadata \
    && yum install -y chromium chromium-headless chromedriver \
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
