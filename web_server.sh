#! /bin/bash
sudo service docker start
docker start family-tree
/var/www/family_tree/.venv/bin/gunicorn --chdir /var/www/family_tree web:app
