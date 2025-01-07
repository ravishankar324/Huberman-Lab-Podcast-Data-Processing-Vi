#!/bin/bash

# Bootstrap action to install required Python dependencies on all nodes

# Upgrade pip
pip3 install --upgrade pip

# Install required Python packages
sudo pip install pandas numpy pyarrow openai python-dotenv boto3

