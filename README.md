# How to install?

- Install Python 3.9:

    https://realpython.com/intro-to-pyenv/
    pyenv local 3.9.18
    pyenv global 3.9.18

- Create an Python environment:

   pyenv virtualenv 3.9.18 ENV

- Go into this environment:

    pyenv local ENV

- Install some other things:

    pip install --upgrade pip
    pip install --upgrade setuptools

- Install dependencies:

    pip install -r requirements.txt
    pip install python-dotenv

- Start port forwarding for postgresql:

    ssh root@167.99.219.233 -L 5431:10.133.75.95:5432

- Start port forwarding for timescaledb:

    ssh -L 5434:10.133.137.239:5432 root@164.92.222.16

# How to run

    export password=X
    export ip=localhost

    source .env
    source ENV/bin/activate
    ./start_dev.sh

# How to test API end points

Use a tool like Postman.

Example call:

```
curl --location --request GET 'http://localhost:5000/stats_v2/availability_stats?start_time=2019-04-04T00:01:00Z&end_time=2019-04-16T00:01:00Z&aggregation_level=15m&zone_ids=51748&group_by=modality' \
--header 'Authorization: Bearer ey..eI'
```

# How to run migrations (importing database structure)

1. wget https://gitlab.com/bikedashboard/importer/-/raw/master/import_model.sql > ~/Downloads/hithere.sql
2. psql deelfietsdashboard -f ~/Downloads/hithere.sql

# How to deploy?

Build api with:

1. `docker build -t registry.gitlab.com/bikedashboard/dashboard-api:<version_number> .` (see kubernetes deployment or https://gitlab.com/bikedashboard/dashboard-api/container_registry/387535 for the previous version).
2. `docker push registry.gitlab.com/bikedashboard/dashboard-api:<version_number>` (make sure you are logged in to gitlab registry)
3. edit deployement with `kubectl edit deployment dashboard-api` replace version_number with the new version number.

# Dependencies

## TimescaleDB

For the zone stats we use TimescaleDB.

For this, add timescale to your postgres instance. 

Documentation over here: https://docs.timescale.com/install/latest/self-hosted/

Related query:

    CREATE EXTENSION IF NOT EXISTS timescaledb;

# Tips & Tricks

## Importing database tables

    psql -h localhost -U deelfietsdashboard -d deelfietsdashboard -f ~/tmp/FILE.sql

## Proxy to production database (PostSQL + TimescaleDB)

    ssh -L 5434:10.133.137.239:5432 root@164.92.222.16
    ssh -L 5431:10.133.75.95:5432 root@auth.deelfietsdashboard.nl