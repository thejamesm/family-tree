#! /bin/bash
#/var/www/family_tree/.venv/bin/gunicorn -w 8 -b 0.0.0.0 'web:app'
sudo service docker start
docker start family-tree
/var/www/family_tree/.venv/bin/gunicorn --chdir /var/www/family_tree -w 8 -b 0.0.0.0 'web:app'
