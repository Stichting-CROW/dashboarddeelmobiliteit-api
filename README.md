# How to deploy?

Build api with:
1. `docker build -t registry.gitlab.com/bikedashboard/dashboard-api:<version_number>` (see kubernetes deployment or https://gitlab.com/bikedashboard/dashboard-api/container_registry/387535 for the previous version).
2. `docker push registry.gitlab.com/bikedashboard/dashboard-api:<version_number>` (make sure you are logged in to gitlab registry)
3. edit deployement with `kubectl edit deployment dashboard-api` replace version_number with the new version number.
