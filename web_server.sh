#! /bin/bash
gunicorn -w 8 -b 0.0.0.0 'web:app'
