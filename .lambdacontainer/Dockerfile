FROM public.ecr.aws/lambda/python:3.9

# Working directory
WORKDIR /app
 
# Instalar dependencias
RUN yum install -y atk cups-libs gtk3 libXcomposite alsa-lib libX11 libxi6 libxtst6 libnss3 fonts-liberation unzip libxdamage1 gtk3-devel \
    libXcursor libXdamage libXext libXi libXrandr curl libXScrnSaver libxrandr2 libxss1 libasound2 libappindicator3-1 libatk-bridge2.0-0 \
    libXtst pango at-spi2-atk libXt xorg-x11-server-Xvfb libXss libatk1.0-0 libgdk-pixbuf2.0-0 libgtk-3-0 libdbus-glib-1-2 libnspr4 gtk3-devel-docs \
    ssl math re xorg-x11-xauth dbus-glib dbus-glib-devel libx11-xcb1 libxcomposite1 nss mesa-libgbm libpango1.0-0 libglib2.0-0 wget libcups2 libgbm1 \
    libdrm2 ca-certificates xdg-utils liberation-fonts vulkan && yum clean all
 
# Descargar chrome y chromedriver
RUN curl -Lo "/tmp/chromedriver-linux64.zip" "https://storage.googleapis.com/chrome-for-testing-public/130.0.6723.93/linux64/chromedriver-linux64.zip" && \
    curl -Lo "/tmp/chrome-headless-shell-linux64.zip" "https://storage.googleapis.com/chrome-for-testing-public/130.0.6723.93/linux64/chrome-headless-shell-linux64.zip" && \
    unzip /tmp/chromedriver-linux64.zip -d ./ && \
    unzip /tmp/chrome-headless-shell-linux64.zip -d ./

# Instala pip y las dependencias de Python
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copiar el código fuente al contenedor
COPY config/
COPY data/
COPY logs/
COPY src/
COPY trigger.py


# Define las variables de entorno para ubicar chrome y chromedriver
ENV CHROME_BIN=/app/chrome-headless-shell-linux64/chrome-headless-shell
ENV CHROMEDRIVER_BIN=/app/chromedriver-linux64/chromedriver


# Asigna permisos al driver y chrome
RUN chmod 777 ./chromedriver-linux64/chromedriver
RUN chmod 777 ./chrome-headless-shell-linux64/chrome-headless-shell


# Imprime en pantalla
RUN ls -l ./
RUN ls -l ./chrome-headless-shell-linux64
RUN ls -l ./chromedriver-linux64
 
# Establecer el punto de entrada
CMD ["src.lambda.lambda_handler"]