from typing import List, Optional, Union

from langchain.docstore.document import Document
from pydantic.v1 import BaseModel, Field


class DocsWithFilterModel(BaseModel):
    query: str = Field(
        ...,
        description="The user query.",
    )
    sent_day_operator: Optional[str] = Field(
        None,
        description="Comparison operator for the day of the month. Accepted values are '$eq', '$ne', '$gt', '$gte', '$lt', '$lte' for a single day, '$in' or '$nin' for a list of days.",
    )
    sent_day: Optional[Union[int, List[int]]] = Field(
        None,
        description="The day of the month the data was sent. For example, 1, 3, 28, [2, 4, 6].",
    )
    sent_day_of_week_operator: Optional[str] = Field(
        None,
        description="Comparison operator for the day of the week. Accepted values are '$eq' for a single day, '$in' or '$nin' for a list of days.",
    )
    sent_day_of_week: Optional[Union[str, List[str]]] = Field(
        None,
        description="The day of the week when the data was sent. For example, 'Sunday', 'Tuesday', ['Wednesday', 'Thursday'].",
    )
    sent_month_operator: Optional[str] = Field(
        None,
        description="Comparison operator for the month. Accepted values are '$eq' for a single month, '$in' or '$nin' for a list of months.",
    )
    sent_month: Optional[Union[str, List[str]]] = Field(
        None,
        description="The month when the data was sent. For example, 'January', 'April', ['May', 'June'].",
    )
    sent_year_operator: Optional[str] = Field(
        None,
        description="Comparison operator for the year. Accepted values are '$eq', '$ne', '$gt', '$gte', '$lt', '$lte' for a single year, '$in' or '$nin' for a list of years.",
    )
    sent_year: Optional[Union[int, List[int]]] = Field(
        None,
        description="The year when the data was sent. For example, 2020, 2022, [2021, 2022].",
    )


class ExtractMetadataModel(BaseModel):
    documents: List[Document] = Field(
        ...,
        description="A list of Document objects from which to extract metadata.",
    )