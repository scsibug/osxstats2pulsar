#!/bin/zsh
# This assumes the appropriate configured python environment is the default:
# $ pyenv global 3.8.6
# Ensure pyenv is in path
PATH=$PATH:/usr/local/bin/
# Init PyEnv
eval "$(pyenv init -)"
# Run the script
python3 osxstats2pulsar.py
