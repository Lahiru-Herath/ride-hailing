FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

USER root
RUN pip install --no-cache-dir \
        pandas \
        pyarrow \
        "protobuf==6.33.0" \
        "psycopg2-binary>=2.9"