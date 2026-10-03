#!/usr/bin/env bash
gunicorn -w 20 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8000 main:app
