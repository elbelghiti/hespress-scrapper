#!/bin/bash

# Activate the virtual environment
source .venv/bin/activate

# Use the first argument if provided; otherwise default to main.py
FILE="${1:-main.py}"

# Run the Python script
python3 "$FILE"
