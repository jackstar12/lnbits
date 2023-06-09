from pydantic import BaseSettings, Extra, Field


class LNbitsSettings(BaseSettings):
    @classmethod
    def validate(cls, val):
        if type(val) == str:
            val = val.split(",") if val else []
        return val

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = Extra.ignore


class DbSettings(LNbitsSettings):
    lnbits_data_folder: str = Field(default="./data")
    lnbits_database_url: str = Field(default=None)


db_settings = DbSettings()
