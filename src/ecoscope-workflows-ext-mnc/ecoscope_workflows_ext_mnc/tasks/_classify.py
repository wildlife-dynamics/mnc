import re
import pandas as pd
from ecoscope_workflows_core.decorators import task
from ecoscope_workflows_core.annotations import AnyDataFrame

@task
def classify_mnc_patrol(
    df: AnyDataFrame, 
    patrol_column: str, 
    new_column: str = "patrol_classification"
) -> AnyDataFrame:
    if df.empty:
        raise ValueError("DataFrame is empty.")
    if patrol_column not in df.columns:
        raise ValueError(f"Column '{patrol_column}' does not exist in the DataFrame.")

    def normalize(text: str) -> str:
        if not isinstance(text, str):
            return ""
        text = text.lower().replace("_", " ").replace("-", " ").strip()
        return re.sub(r"\s+", " ", text)

    def classify(value):
        p = normalize(value)
        if not p:
            return "unknown"

        if "foot" in p:
            return "foot"
        if any(word in p for word in ["vehicle", "car"]):
            return "vehicle"
        if any(word in p for word in ["motor", "motorbike", "bike", "motorcycle"]):
            return "motorcycle"

        return "unknown"

    result = df.copy()
    result[new_column] = result[patrol_column].apply(classify)
    return result
