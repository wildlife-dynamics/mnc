from functools import reduce
from operator import add, floordiv, mod, mul, pow, sub, truediv
from typing import Annotated, Literal, cast
import numpy as np
from pydantic import Field
from wt_registry import register
from ecoscope.platform.annotations import AnyDataFrame

operations = {
    "add": add,
    "subtract": sub,
    "multiply": mul,
    "divide": truediv,
    "floor_divide": floordiv,
    "modulo": mod,
    "power": pow,
    "min": np.minimum,
    "max": np.maximum,
}
Operations = Literal[
    "add",
    "subtract",
    "multiply",
    "divide",
    "floor_divide",
    "modulo",
    "power",
    "min",
    "max",
]


@register()
def apply_arithmetic_operation_over_rows(
    df: AnyDataFrame,
    columns: Annotated[
        list[str],
        Field(description="The columns to combine, in order", min_length=2),
    ],
    output_column: Annotated[str, Field(description="The output column name")],
    operation: Annotated[Operations, Field(description="The arithmetic operation to apply")],
) -> AnyDataFrame:
    op = operations[operation]
    df[output_column] = reduce(op, (df[c] for c in columns))
    return cast(AnyDataFrame, df)
