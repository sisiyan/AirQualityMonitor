#!/bin/bash

until python3 app.py; do
    echo "Server 'python3 app.py' crashed with exit code $?.  Respawning.." >&2
    sleep 1
done
