FROM python:3.12-alpine

COPY eggd_conductor_monitor.py .
COPY requirements.txt .

RUN mkdir /logs/

RUN apk add gcc musl-dev linux-headers python3-dev

# - Install requirements
# - Delete unnecessary Python files
# - Alias command `s3_upload` to `python3 s3_upload.py` for convenience
RUN \
    pip install --quiet --upgrade pip && \
    pip install -r requirements.txt && \
    echo "Delete python cache directories" 1>&2 && \
    find /usr/local/lib/python3.12 \( -iname '*.c' -o -iname '*.pxd' -o -iname '*.pyd' -o -iname '__pycache__' \) | xargs rm -rf {}