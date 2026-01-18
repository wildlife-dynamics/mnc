import logging
from typing import Annotated, cast
from pydantic.json_schema import SkipJsonSchema
from ecoscope_workflows_core.annotations import AdvancedField, AnyDataFrame
from ecoscope_workflows_core.decorators import task
from ecoscope_workflows_core.tasks.transformation._mapping import RenameColumn

logger = logging.getLogger(__name__)

@task
def transform_columns(
    df: AnyDataFrame,
    drop_columns: Annotated[
        list[str] | SkipJsonSchema[None],
        AdvancedField(default=[], description="List of columns to drop."),
    ] = None,
    retain_columns: Annotated[
        list[str] | SkipJsonSchema[None],
        AdvancedField(
            default=[],
            description="""List of columns to retain with the order specified by the list.
                        Keep all the columns if the list is empty.""",
        ),
    ] = None,
    rename_columns: Annotated[
        list[RenameColumn] | SkipJsonSchema[dict[str, str]] | SkipJsonSchema[None],
        AdvancedField(default={}, description="Dictionary of columns to rename."),
    ] = None,
    required_columns: Annotated[
        list[str] | SkipJsonSchema[None],
        AdvancedField(
            default=[],
            description="""List of columns that must be present in the DataFrame.
                        If any required column is missing, an error will be raised.
                        Rename operation will only apply to columns that exist.""",
        ),
    ] = None,
    skip_missing_rename: Annotated[
        bool,
        AdvancedField(
            default=True,
            description="""If True, skip renaming of columns that don't exist in the DataFrame.
                        If False, raise an error when trying to rename missing columns.""",
        ),
    ] = True,
) -> AnyDataFrame:
    """
    Maps and transforms the columns of a DataFrame based on the provided parameters. The order of the operations is as
    follows: drop columns, retain/reorder columns, and rename columns.
    
    Args:
        df (AnyDataFrame): The input DataFrame to be transformed.
        drop_columns (list[str]): List of columns to drop from the DataFrame.
        retain_columns (list[str]): List of columns to retain. The order of columns will be preserved.
        rename_columns (dict[str, str]): Dictionary of columns to rename.
        required_columns (list[str]): List of columns that must be present. Error raised if any are missing.
        skip_missing_rename (bool): If True, skip renaming columns that don't exist. Default is True.
        
    Returns:
        AnyDataFrame: The transformed DataFrame.
        
    Raises:
        KeyError: If any required columns are missing or if rename columns are missing and skip_missing_rename=False.
    """
    # Check for required columns first
    if required_columns:
        missing_required = [col for col in required_columns if col not in df.columns]
        if missing_required:
            raise KeyError(
                f"Required columns {missing_required} not found in DataFrame. "
                f"Available columns: {list(df.columns)}"
            )
    
    # Drop columns
    if drop_columns:
        if "geometry" in drop_columns:
            logger.warning(
                "'geometry' found in drop_columns, which may affect spatial operations."
            )
        df = df.drop(columns=drop_columns)
    
    # Retain/reorder columns
    if retain_columns:
        if any(col not in df.columns for col in retain_columns):
            raise KeyError(f"Columns {retain_columns} not all found in DataFrame.")
        df = df.reindex(columns=retain_columns)  # type: ignore[assignment]
    
    # Rename columns
    if rename_columns:
        if isinstance(rename_columns, list):
            rename_columns = {
                item.original_name: item.new_name for item in rename_columns
            }
        
        if "geometry" in rename_columns.keys():
            logger.warning(
                "'geometry' found in rename_columns, which may affect spatial operations."
            )
        
        # Filter rename_columns to only include columns that exist in df
        existing_rename_columns = {
            old_name: new_name
            for old_name, new_name in rename_columns.items()
            if old_name in df.columns
        }
        
        # Check for missing columns
        missing_rename_columns = [
            col for col in rename_columns.keys() if col not in df.columns
        ]
        
        if missing_rename_columns:
            if skip_missing_rename:
                logger.warning(
                    f"Skipping rename for missing columns: {missing_rename_columns}. "
                    f"Available columns: {list(df.columns)}"
                )
            else:
                raise KeyError(
                    f"Columns {missing_rename_columns} not found in DataFrame. "
                    f"Existing columns: {list(df.columns)}"
                )
        
        # Apply rename only for existing columns
        if existing_rename_columns:
            df = df.rename(columns=existing_rename_columns)  # type: ignore[assignment]
    
    return cast(AnyDataFrame, df)