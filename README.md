# How to install?

- Install Python 3.9:

    sudo apt-get install python3.9 python3.9-dev python3.9-venv

- Create an Python environment:

    python3.9 -m venv ENV

- Go into this environment:

    source ENV/bin/activate

- Install some other things:

    pip install --upgrade pip
    pip install --upgrade setuptools

- Install dependencies:

    pip install -r requirements.txt

# How to run

    export password=X
    export ip=localhost

    source ENV/bin/activate
    ./start_dev.sh

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
