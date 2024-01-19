"""This is an example blocks module"""
from prefect import Task


class PrintMessageBlock(Task):
    def __init__(self, message: str = "Hello, World!", **kwargs):
        self.message = message
        super().__init__(**kwargs)

    def run(self):
        print(self.message)
