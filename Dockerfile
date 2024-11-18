
FROM python:3.11.2-bullseye
ENV PIP_NO_CACHE_DIR off
ENV PIP_DISABLE_PIP_VERSION_CHECK on
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV COLUMNS 80
RUN apt-get update \
 && apt-get install -y --force-yes \
 nano python3-pip gettext chrpath libssl-dev libxft-dev \
 libfreetype6 libfreetype6-dev  libfontconfig1 libfontconfig1-dev\
  && rm -rf /var/lib/apt/lists/*
WORKDIR /app/
COPY requirements.txt /app/
COPY daily_scraping_module.py /app/
COPY daily_scraping_fetch_and_parse.py /app/
RUN pip install -r requirements.txt
