from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://integer:integer@localhost:55432/integer"
    provider_webhook_secret: str = "dev-provider-secret"
    payment_webhook_secret: str = "dev-payment-secret"
    provider_timeout: int = 60
    alpha_url: str = "http://alpha:8000"
    beta_url: str = "http://beta:8000"
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
