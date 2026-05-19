from pydantic import BaseModel
from typing import List

class ColumnRequest(BaseModel):
    job_id: str
    user_selected_columns: List[str]

    