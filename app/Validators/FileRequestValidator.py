from pydantic import BaseModel

class FileRequest(BaseModel):
    directory: str
    file: str