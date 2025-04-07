import json
import datetime
from threading import Event
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple
import scipy  # noqa: ignore=F401
from predictor import RHDiskFailurePredictor, DevSmartT

# initialize appropriate disk failure predictor model
obj_predictor = RHDiskFailurePredictor()
obj_predictor.initialize("models/redhat")

predict_datas: List[DevSmartT] = []

# Open and read the JSON file
with open('predict_datas.json', 'r') as file:
    loaded_data = json.load(file)
    for data in loaded_data:
        predict_datas.append(data)

predicted_result = obj_predictor.predict(predict_datas)

print(predicted_result)