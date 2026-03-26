FROM python:3.12-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=on \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app



RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev curl git procps netcat-traditional \
    chromium chromium-driver fonts-liberation libnss3 libasound2 \
    libx11-6 libx11-xcb1 libxcb-dri3-0 libxcomposite1 libxcursor1 \
    libxdamage1 libxfixes3 libxi6 libxrandr2 libxrender1 libxss1 \
    libxtst6 libatk-bridge2.0-0 libgtk-3-0 libgbm1 libdrm2 \
    libatspi2.0-0 libxkbcommon0 libcups2 xdg-utils ca-certificates \
    xvfb x11vnc xauth \
    tesseract-ocr tesseract-ocr-eng libtesseract-dev \
    npm \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
&& apt-get install -y nodejs

   
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER=/usr/bin/chromedriver

RUN pip install --no-cache-dir poetry==1.8.3

# NPM related installs 
WORKDIR /opt/app_node
COPY package.json ./
RUN npm install --omit=dev --no-fund \
 && npx playwright install chromium

# Make Node resolve modules from the stable path
ENV NODE_PATH=/opt/app_node/node_modules
ENV PATH=/opt/app_node/node_modules/.bin:${PATH}

# App code and Python deps
WORKDIR /app
COPY . .
RUN poetry install --no-interaction
RUN poetry run playwright install chromium