from abc import ABC, abstractmethod

import polars as pl


class BaseService(ABC):
    """
    Abstract Base Class for all table orchestration services.
    Enforces a standard lifecycle: validate -> compute -> write.
    """

    @abstractmethod
    def validate(self, data: pl.DataFrame) -> pl.DataFrame:
        """
        Pre-processing checks. Should raise errors or return cleaned data.
        """
        pass

    @abstractmethod
    def compute_state(self, valid_data: pl.DataFrame) -> pl.DataFrame:
        """
        Applies domain logic (transformations, SCD2, etc.) to produce the state to be written.
        """
        pass

    @abstractmethod
    def write(self, result: pl.DataFrame) -> None:
        """
        Orchestrates the persistence of the result to the repository.
        """
        pass

    def update(self, data: pl.DataFrame) -> None:
        """
        The template method orchestrating the update lifecycle.
        """
        valid_data = self.validate(data)
        result = self.compute_state(valid_data)
        self.write(result)
