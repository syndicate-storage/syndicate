class ObjectManager(object):
    """
    This is a base object manager making sure all sub-classes implement the
    right methods and properties.
    """
    supports_cursors = None

    @property
    def cache_key(self):
        """
        This should return a string that can be used as a unique cache key for
        a query considering its set properties like order, filters, etc.
        """
        raise NotImplemented()

    def starting_cursor(self, cursor):
        """
        This method should be used to set a cursor/token before actually doing
        the query by accessing a subset of a query's elements.
        """
        if self.supports_cursors:
            raise NotImplemented()

    @property
    def next_cursor(self):
        """
        Returns the cursor/token after a query has been made. The cursor needs
        to be cached on the object until this method is called.
        """
        if self.supports_cursors:
            raise NotImplemented()

    def __getitem__(self, value):
        """
        Doing the actual query to the given backend (DB, API, etc.), caching the
        next cursor so that it can be retrieved via self.next_cursor().

        Returns a list of objects. (no queryset or similar please!)
        """
        raise NotImplemented()

    def contains_more_objects(self, next_batch_cursor):
        """
        Makes another query to check if there are any more objects available
        doing the same query with the passed in cursor (usually equal to
        self.next_cursor)

        Returns a boolean stating if it contains more or not.
        """
        raise NotImplemented()
