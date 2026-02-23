import pandas as pd
import dolphindb as ddb
from src.entity.Source import Source

class Result(Source):
    def __init__(self, session: ddb.session):
        super().__init__(session)
        self.resultDB = ""
        self.resultTB = ""
        self.re