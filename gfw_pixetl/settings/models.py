from pydantic import BaseSettings


class EnvSettings(BaseSettings):
    def env_dict(self):
        env = self.dict(exclude_none=True, exclude_unset=True)
        return {key.upper(): value for key, value in env.items()}

    class Config:
        case_sensitive = False
        validate_assignment = True
