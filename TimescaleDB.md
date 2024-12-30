Run the TimescaleDB Docker image

docker pull timescale/timescaledb-ha:pg17

Run the container

docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg17

psql -d "postgres://postgres:password@localhost/postgres"
