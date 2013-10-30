from djangoappengine.db.utils import set_cursor, get_cursor

from ..utils import supports_cursor
from .base import ObjectManager


class DjangoNonrelManager(ObjectManager):
    """
        Object manager handling normal GAE db querysets.

        TODO: Somre more specific manager tests would be nice. Not that
              necessary though.
    """
    def __init__(self, queryset):
        self.queryset = queryset
        self.supports_cursors = supports_cursor(queryset)
        self._start_cursor = None
        self._latest_cursor = None

    @property
    def cache_key(self):
        """
            Returns a key that can be used to cache this particular object manager.
            I.e. a unique string for the given queryset.
        """
        return " ".join([
            str(self.queryset.query.where),
            str(self.queryset.query.order_by),
            str(self.queryset.query.low_mark),
            str(self.queryset.query.high_mark)
        ]).replace(" ", "_")

    def starting_cursor(self, cursor):
        """
            Let's you set the starting cursor. Should be called before actually
            calling __getitem__()
        """
        self._start_cursor = cursor
        self._latest_cursor = None

    @property
    def next_cursor(self):
        """
            Returns the end cursor after doing the query and validating it.
        """
        return self._latest_cursor

    def __getitem__(self, value):
        """
            Does the query, saves the cursor for the next query to self and
            returns the objects in form of a list.
        """
        query = self.queryset.all()[value]
        if self._start_cursor:
            query = set_cursor(query, start=self._start_cursor)
            self._start_cursor = None

        obj_list = list(query)
        try:
            self._latest_cursor = get_cursor(query)
        except TypeError:
            # get_cursor() tries to call .urlsafe() on the cursor with fails
            # if the cursor is None. So we save None if it's None. Makes sense.
            self._latest_cursor = None

        return obj_list

    def contains_more_objects(self, next_batch_cursor):
        """
            Returns a boolean telling if there are more objects in the queryset
            or if there aren't.
        """
        query = self.queryset.all().values_list('pk')
        query = set_cursor(query, start=next_batch_cursor)

        try:
            query[0]
            return True
        except IndexError:
            return False
