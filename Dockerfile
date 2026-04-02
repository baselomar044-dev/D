FROM python:3.12-slim

WORKDIR /app

# System deps for ezdxf, shapely, scipy
RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    libgeos-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY config/ config/
COPY samples/ samples/

ENTRYPOINT ["python", "-m", "src.main"]
CMD ["--sample", "--type", "G+1", "--output", "/app/output/boq.xlsx"]
