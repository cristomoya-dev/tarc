# config.py  — configuración central leída desde variables de entorno
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    database_url:   str
    openai_api_key: str
    embed_model:    str   = "text-embedding-3-small"
    embed_dim:      int   = 1536
    llm_model:      str   = "gpt-4o-mini"
    pdf_dir:        str   = "/data/pdfs"
    log_level:      str   = "info"

    class Config:
        env_file = ".env"

settings = Settings()
