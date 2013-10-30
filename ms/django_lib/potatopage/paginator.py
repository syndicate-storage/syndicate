import logging
from math import ceil

from django.core.cache import cache
from django.core.paginator import (
    Paginator,
    EmptyPage,
    PageNotAnInteger,
    Page
)

from .object_managers.base import ObjectManager


class CursorNotFound(Exception):
    pass

class UnifiedPaginator(Paginator):
    def __init__(self, object_list, per_page, batch_size=1, readahead=True, *args, **kwargs):
        """
            batch_size - The steps (in pages) that cursors are cached. A batch_size
            of 1 means that a cursor is cached for the start of each page.

            readahead - When we store a cursor, try to access it to see if there is
            anything actually there. This makes page counts behave correctly at the
            cost of an extra keys_only query using the cursor as an offset. This isn't
            used on IN queries as that would be slow as molasses
        """

        self._batch_size = batch_size
        self._readahead = readahead

        if not isinstance(object_list, ObjectManager):
            raise TypeError('%s doesn\'t support standard object lists. Please make sure it\'s a subclass of %s' % (self.__class__.__name__, ObjectManager.__name__))

        if not object_list.supports_cursors:
            self._readahead = False

        super(UnifiedPaginator, self).__init__(object_list, per_page, *args, **kwargs)

    def _get_final_page(self):
        key = "|".join([self.object_list.cache_key, "LAST_PAGE"])
        return cache.get(key)

    def _put_final_page(self, page):
        key = "|".join([self.object_list.cache_key, "LAST_PAGE"])
        cache.set(key, page)

    def _get_final_item(self):
        key = "|".join([self.object_list.cache_key, "LAST_ITEM"])
        return cache.get(key)

    def _put_final_item(self, item):
        key = "|".join([self.object_list.cache_key, "LAST_ITEM"])
        cache.set(key, item)

    def _get_known_page_count(self):
        key = "|".join([self.object_list.cache_key, "KNOWN_PAGE_MAX"])
        return cache.get(key)

    def _put_known_page_count(self, count):
        key = "|".join([self.object_list.cache_key, "KNOWN_PAGE_MAX"])
        return cache.set(key, count)

    def _get_known_items_count(self):
        """ Use this when you don't know how many pages there is """
        key = "|".join([self.object_list.cache_key, "KNOWN_ITEMS_MAX"])
        return cache.get(key)

    def _put_known_items_count(self, count):
        key = "|".join([self.object_list.cache_key, "KNOWN_ITEMS_MAX"])
        return cache.set(key, count)

    def _put_cursor(self, zero_based_page, cursor):
        if not self.object_list.supports_cursors or cursor is None:
            return

        logging.info("Storing cursor for page: %s" % (zero_based_page))
        key = "|".join([self.object_list.cache_key, str(zero_based_page)])
        cache.set(key, cursor)

    def _get_cursor(self, zero_based_page):
        logging.info("Getting cursor for page: %s" % (zero_based_page))
        key = "|".join([self.object_list.cache_key, str(zero_based_page)])
        result = cache.get(key)
        if result is None:
            raise CursorNotFound("No cursor available for %s" % zero_based_page)
        return result

    def has_cursor_for_page(self, page):
        try:
            self._get_cursor(page-1)
            return True
        except CursorNotFound:
            return False

    def validate_number(self, number):
        "Validates the given 1-based page number."
        try:
            number = int(number)
        except (TypeError, ValueError):
            raise PageNotAnInteger('That page number is not an integer')
        if number < 1:
            raise EmptyPage('That page number is less than 1')

        return number

    def _find_nearest_page_with_cursor(self, current_page):
        #Find the next page down that should be storing a cursor
        page_with_cursor = current_page
        while page_with_cursor % self._batch_size != 0:
            page_with_cursor -= 1
        return page_with_cursor

    def _get_cursor_and_offset(self, page):
        """ Returns a cursor and offset for the page. page is zero-based! """

        offset = 0
        cursor = None
        page_with_cursor = self._find_nearest_page_with_cursor(page)

        if self.object_list.supports_cursors:
            if page_with_cursor > 0:
                try:
                    cursor = self._get_cursor(page_with_cursor)
                    logging.info("Using existing cursor from memcache")
                except CursorNotFound:
                    logging.info("Couldn't find a cursor")
                    #No cursor found, so we just return the offset old-skool-style.
                    cursor = None

        offset = (page - page_with_cursor) * self.per_page

        return cursor, offset

    def _process_batch_hook(self, batch_results, zero_based_page, cursor, offset):
        """ Override this in the subclass to cache results etc."""
        pass

    def page(self, number):
        number = self.validate_number(number)

        cursor, offset = self._get_cursor_and_offset(number-1)

        if cursor:
            self.object_list.starting_cursor(cursor)
            results = self.object_list[:(self.per_page * self._batch_size)]
        else:
            bottom = (self.per_page * self._find_nearest_page_with_cursor(number-1))
            top = bottom + (self.per_page * self._batch_size)
            #No cursor, so grab the full batch
            results = self.object_list[bottom:top]

        self._process_batch_hook(results, number-1, cursor, offset)

        nearest_page_with_cursor = self._find_nearest_page_with_cursor(number-1)
        next_cursor = None
        if self.object_list.supports_cursors:
            #Store the cursor at the start of the NEXT batch
            next_cursor = self.object_list.next_cursor
            self._put_cursor(nearest_page_with_cursor + self._batch_size, next_cursor)

        batch_result_count = len(results)

        actual_results = results[offset:offset + self.per_page]

        if not actual_results:
            if number == 1 and self.allow_empty_first_page:
                pass
            else:
                raise EmptyPage('That page contains no results')

        # Calculate known_page_count and cache it if necessary.
        known_page_count = int(nearest_page_with_cursor + ceil(batch_result_count / float(self.per_page)))

        if known_page_count >= self._get_known_page_count():
            if next_cursor and self._readahead:
                if self.object_list.contains_more_objects(next_cursor):
                    known_page_count += 1
                else:
                    self._put_final_page(known_page_count)
            elif batch_result_count == self._batch_size * self.per_page:
                # If we got back exactly the right amount, we assume there is at least
                # one more page.
                known_page_count += 1

            self._put_known_page_count(known_page_count)

        # Calculate known_item_count and if it's the last item
        known_item_count = int(nearest_page_with_cursor * self.per_page + batch_result_count)

        if known_item_count > self._get_known_items_count():
            self._put_known_items_count(known_item_count)

        if batch_result_count < (self.per_page * self._batch_size):
            # No need to read ahead for one item, it won't be 100% accurate anyway.
            self._put_final_item(known_item_count)

        return UnifiedPage(actual_results, number, self)

    def _get_count(self):
        raise NotImplemented("Not available in %s" % self.__class__.__name__)

    def _get_num_pages(self):
        raise NotImplemented("Not available in %s" % self.__class__.__name__)


class UnifiedPage(Page):
    def __init__(self, object_list, number, paginator):
        super(UnifiedPage, self).__init__(object_list, number, paginator)

    def __repr__(self):
        """ Overwrite paginator's repr, so no Exception gets thrown
            because the number of pages is unknown.
        """
        return '<Page %s>' % (self.number)

    def has_next(self):
        return self.number < self.paginator._get_known_page_count()

    def start_index(self):
        """ Override to prevent returning 0 """
        if self.number == 0:
            return 1
        return (self.paginator.per_page * (self.number - 1)) + 1

    def end_index(self):
        """ Override to prevent a call to _get_count """
        if self.has_next():
            return self.number * self.paginator.per_page
        else:
            # Special case for a last page when the page has less items then a per_page value
            final_item_count = self.paginator._get_final_item()
            if not final_item_count:
                return self.paginator._get_known_items_count()
            return final_item_count

    def final_page_visible(self):
        return self.paginator._get_final_page() in self.available_pages()

    def available_pages(self, limit_to_batch_size=True):
        """
            Returns a list of sorted integers that represent the
            pages that should be displayed in the paginator. In relation to the
            current page. For example, if this page is page 3, and batch_size is 5
            we get the following:

            [ 1, 2, *3*, 4, 5, 6, 7, 8 ]

            If we then choose page 7, we get this:

            [ 2, 3, 4, 5, 6, *7*, 8, 9, 10, 11, 12 ]

            If limit_to_batch_size is False, then you always get all known pages
            this will generally be the same for the upper count, but the results
            will always start at 1.
        """
        min_page = (self.number - self.paginator._batch_size) if limit_to_batch_size else 1
        if min_page < 1:
            min_page = 1

        max_page = min(self.number + self.paginator._batch_size, self.paginator._get_known_page_count())
        return list(xrange(min_page, max_page + 1))

    def __repr__(self):
        return '<UnifiedPage %s>' % self.number


class DjangoNonrelPaginator(UnifiedPaginator):
    """
        Paginator that uses a Django-nonrel's GAE db queries to retrieve the objects.
    """
    def __init__(self, queryset, *args, **kwargs):
        # Inline import otherwise importing the UnifiedPaginator would fail
        # because of this import!
        from object_managers.gae_db import DjangoNonrelManager
        object_list = DjangoNonrelManager(queryset)
        super(DjangoNonrelPaginator, self).__init__(object_list, *args, **kwargs)


class GaeNdbPaginator(UnifiedPaginator):
    """
        Paginator using GAE's NDB.
    """
    def __init__(self, query, *args, **kwargs):
        from object_managers.ndb_api import GaeNdbModelManager
        object_list = GaeNdbModelManager(query)
        super(GaeNdbPaginator, self).__init__(object_list, *args, **kwargs)
