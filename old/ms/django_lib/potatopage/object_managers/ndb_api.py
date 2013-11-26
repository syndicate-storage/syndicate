from google.appengine.datastore.datastore_query import Cursor

from .base import ObjectManager


class GaeNdbModelManager(ObjectManager):
    """
        An object manager for ndb models.

        TODO: Writing some manager specific tests. Currently we always just test
              the paginator itself.
    """
    supports_cursors = True

    def __init__(self, query):
        self.query = query
        self._starting_cursor = None
        self._contians_more_entities = None
        self._latest_end_cursor = None

    @property
    def cache_key(self):
        """
            Returns a key that can be used to cache this particular object manager.
            I.e. a unique string for the given query.
        """
        return " ".join([
            str(self.query._Query__kind),
            str(self.query._Query__ancestor),
            str(self.query._Query__filters),
            str(self.query._Query__orders),
            str(self.query._Query__app),
            str(self.query._Query__namespace)
        ]).replace(" ", "_")

    def starting_cursor(self, cursor):
        """
            Let's you set the starting cursor. Should be called before actually
            calling __getitem__()
        """
        self._starting_cursor = Cursor(urlsafe=cursor)
        # we need to set those to None, otherwise a second query with the same
        # paginator would fail as the cursors are already set.
        self._latest_end_cursor = None
        self._contians_more_entities = None


    @property
    def next_cursor(self):
        """
            Returns the end cursor after doing the query and validating it.
        """
        return self._latest_end_cursor

    def __getitem__(self, value):
        """
            Does the query, saves the cursor for the next query to self and
            returns the objects in form of a list.
        """
        if isinstance(value, slice):
            start, max_items = value.start, value.stop

        if isinstance(value, int):
            max_items = value

        entities, cursor, more = self.query.fetch_page(
            max_items,
            start_cursor=self._starting_cursor
        )

        self._starting_cursor = None
        if cursor is not None:
            self._latest_end_cursor = cursor.urlsafe()
        self._contians_more_entities = more

        return entities[value]

    def contains_more_objects(self, next_cursor):
        """
            Returns a boolean telling if there are more objects in the queryset
            or if there aren't.
        """
        if self._contians_more_entities is not None:
            return self._contians_more_entities

        entities, cursor, more = self.query.fetch_page(
            1,
            start_cursor=next_cursor
        )

        entity_list = list(entities)
        return bool(entity_list)