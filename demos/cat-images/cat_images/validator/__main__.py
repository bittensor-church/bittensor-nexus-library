from .validator import Validator
from .validator_settings import CatValidatorSettings

if __name__ == "__main__":
    Validator.run(settings_class=CatValidatorSettings)
