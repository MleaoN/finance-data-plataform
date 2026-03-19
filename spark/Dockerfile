FROM apache/spark:3.5.1-python3

USER root

# Install Python dependencies
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary python-dotenv


# Add PostgreSQL JDBC driver
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Create a real home directory for the spark user
RUN mkdir -p /home/spark && \
    chown -R spark:spark /home/spark

ENV HOME=/home/spark
ENV PATH="/opt/spark/bin:${PATH}"

USER spark