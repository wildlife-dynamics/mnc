import re
import pandas as pd
from ecoscope_workflows_core.decorators import task
from ecoscope_workflows_core.annotations import AnyDataFrame

@task
def classify_mnc_patrol(
    df: AnyDataFrame, 
    patrol_col: str, 
    new_col: str = "patrol_classification"
    ) -> AnyDataFrame:
    def _classify(patrol_type):
        """Internal classification logic."""
        if not isinstance(patrol_type, str) or pd.isna(patrol_type) or patrol_type.strip() == "":
            return "Unknown"
        
        p = patrol_type.lower().replace("_", " ").replace("-", " ").strip()
        p = re.sub(r"\s+", " ", p)
        
        # Classification rules
        if "foot" in p:
            return "Foot"
        elif any(word in p for word in ["vehicle", "car"]):
            return "Vehicle"
        elif any(word in p for word in ["motor", "motorbike", "bike", "motorcycle"]):
            return "Motorcycle"
        else:
            return "Unknown"

    result_df = df.copy()
    result_df[new_col] = result_df[patrol_col].apply(_classify)
    return result_df