FROM apache/airflow:3.3.2

# Extra deps for processing/batch_reconciliation.py. Follows the official "extending the image"
# pattern: install as the airflow user, pin apache-airflow to the base version so pip can't
# silently up/downgrade it, and use the constraints file the image was built with (D26).
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
        pandas \
        pyarrow \
        --constraint "${HOME}/constraints.txt"