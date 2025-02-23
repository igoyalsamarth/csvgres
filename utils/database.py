from transformer.controller import Csvgres
from functools import lru_cache

@lru_cache()
def get_db():
    return Csvgres() 