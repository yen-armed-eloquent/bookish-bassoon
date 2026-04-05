FROM python:3.11-slim

WORKDIR /app

# Rclone aur dependencies install karein
RUN apt-get update && apt-get install -y curl unzip procps && \
    curl https://rclone.org/install.sh | bash && \
    apt-get clean

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scraper.py .

# Rclone config path create karna
RUN mkdir -p /root/.config/rclone/

CMD ["python", "scraper.py"]