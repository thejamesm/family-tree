#! /bin/bash
/var/www/family_tree/.venv/bin/gunicorn -w 8 -b 0.0.0.0 'web:app'
