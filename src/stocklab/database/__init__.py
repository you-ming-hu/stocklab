import json
import pathlib

print(pathlib.Path(__file__).parent.joinpath('config.json').exists())
