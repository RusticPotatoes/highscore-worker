from abc import ABC, abstractmethod

from database.database import get_session


class ABCRepo(ABC):
    """
    Abstract base class for repositories.
    """

    async def _get_session(self):
        return await get_session()

    @abstractmethod
    async def create(self, data):
        """
        Creates a new entity.

        Raises:
            NotImplementedError: This method must be implemented in subclasses.
        """
        raise NotImplementedError("Subclasses must implement the create method")

    @abstractmethod
    async def request(self, id):
        """
        Retrieves an entity by its ID.

        Raises:
            NotImplementedError: This method must be implemented in subclasses.
        """
        raise NotImplementedError("Subclasses must implement the request method")

    @abstractmethod
    async def update(self, id, data):
        """
        Updates an existing entity.

        Raises:
            NotImplementedError: This method must be implemented in subclasses.
        """
        raise NotImplementedError("Subclasses must implement the update method")

    @abstractmethod
    async def delete(self, id):
        """
        Deletes an entity by its ID.

        Raises:
            NotImplementedError: This method must be implemented in subclasses.
        """
        raise NotImplementedError("Subclasses must implement the delete method")
