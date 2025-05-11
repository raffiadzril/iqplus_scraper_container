FROM apache/airflow:2.8.1-python3.10

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y wget unzip curl gnupg ca-certificates && \
    apt-get install -y chromium chromium-driver fonts-liberation libasound2 libatk-bridge2.0-0 \
    libatk1.0-0 libcups2 libdbus-1-3 libgdk-pixbuf2.0-0 libnspr4 libnss3 libx11-xcb1 libxcomposite1 \
    libxdamage1 libxrandr2 xdg-utils libxss1 --no-install-recommends && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Ensure ChromeDriver is in PATH
ENV PATH="/usr/bin:${PATH}"

# Create symlink for ChromeDriver if needed
RUN if [ -f /usr/bin/chromedriver ] && [ ! -f /usr/local/bin/chromedriver ]; then \
    ln -s /usr/bin/chromedriver /usr/local/bin/chromedriver; \
    fi

# Kembali ke user airflow untuk pip install
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
