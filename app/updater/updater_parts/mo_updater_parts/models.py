from pydantic import BaseModel, Field


class OperationResponse(BaseModel):
    update: list = Field(default_factory=list)
    create: list = Field(default_factory=list)
    delete: list = Field(default_factory=list)
