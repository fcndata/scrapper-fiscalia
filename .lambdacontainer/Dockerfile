<<<<<<< HEAD
# AWS provee imágenes base que ya incluyen el Runtime Interface Client (RIC) necesario para que Lambda se comunique con tu contenedor.
FROM public.ecr.aws/lambda/python:3.10

# 1️⃣ Instalar dependencias del sistema necesarias para Chromium
RUN yum install -y \
      atk cups-libs gtk3 libXcomposite alsa-lib libX11 libXi libXtst libnss3 fonts-liberation unzip \
      libXdamage gtk3-devel libXcursor libXrandr libxss libasound2 libappindicator3-1 libatk-bridge2.0-0 \
      pango at-spi2-atk libXt xorg-x11-server-Xvfb libgdk-pixbuf2.0-0 libdbus-glib-1-2 libnspr4 \
      mesa-libgbm libpango1.0-0 libglib2.0-0 wget libcups2 libgbm1 libdrm2 ca-certificates xdg-utils \
      liberation-fonts vulkan && \
    yum clean all
=======
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
>>>>>>> origin/feature/cloud_base

# 2️⃣ Descargar Chrome headless y Chromedriver, y asignar permisos
RUN curl -Lo /tmp/chromedriver-linux64.zip \
      https://storage.googleapis.com/chrome-for-testing-public/130.0.6723.93/linux64/chromedriver-linux64.zip && \
    curl -Lo /tmp/chrome-headless-shell-linux64.zip \
      https://storage.googleapis.com/chrome-for-testing-public/130.0.6723.93/linux64/chrome-headless-shell-linux64.zip && \
    unzip /tmp/chromedriver-linux64.zip -d /opt/chromedriver && \
    unzip /tmp/chrome-headless-shell-linux64.zip -d /opt/chrome-headless-shell && \
    chmod +x /opt/chromedriver/chromedriver && \
    chmod +x /opt/chrome-headless-shell/chrome-headless-shell && \
    ln -s /opt/chrome-headless-shell/chrome-headless-shell /usr/bin/chromium-browser && \
    ln -s /opt/chromedriver/chromedriver /usr/bin/chromedriver

# 3️⃣ Variables de entorno para Chromium
ENV CHROME_BIN=/usr/bin/chromium-browser \
    CHROMEDRIVER_BIN=/usr/bin/chromedriver

# Set working directory to Lambda task root
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy and install Python dependencies
COPY requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# 5️⃣ Copiar código de la aplicación
COPY . ${LAMBDA_TASK_ROOT}/

# Define the Lambda handler entry point
# Assumes you have created lambda_handler.py with a function named lambda_handler
CMD ["trigger.main"]
