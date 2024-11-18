FROM python:3.8-alpine

COPY ./ ./

# - Install requirements
# - Delete unnecessary Python files
# - Alias command `s3_upload` to `python3 s3_upload.py` for convenience
RUN \
    pip install --quiet --upgrade pip && \
    pip install -r requirements.txt && \
    echo "Delete python cache directories" 1>&2 && \
    find /usr/local/lib/python3.8 \( -iname '*.c' -o -iname '*.pxd' -o -iname '*.pyd' -o -iname '__pycache__' \) | \
    xargs rm -rf {} && \
    pip install -r requirements.txt