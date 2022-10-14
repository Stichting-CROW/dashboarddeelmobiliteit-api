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

        source ENV/bin/activate
        ./start_dev.sh

# How to deploy?

Build api with:

1. `docker build -t registry.gitlab.com/bikedashboard/dashboard-api:<version_number> .` (see kubernetes deployment or https://gitlab.com/bikedashboard/dashboard-api/container_registry/387535 for the previous version).
2. `docker push registry.gitlab.com/bikedashboard/dashboard-api:<version_number>` (make sure you are logged in to gitlab registry)
3. edit deployement with `kubectl edit deployment dashboard-api` replace version_number with the new version number.

--

gebleven bij: sudo adduser deelfietsdashboard
vraag: wat is het ww?

https://www.digitalocean.com/community/tutorials/how-to-install-postgresql-on-ubuntu-20-04-quickstart
