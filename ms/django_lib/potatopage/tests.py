from google.appengine.ext import ndb

from django.db import models
from django.test import TestCase

import mock

from potatopage.paginator import (
    DjangoNonrelPaginator,
    GaeNdbPaginator,
    EmptyPage
)


class DjangoNonrelPaginationModel(models.Model):
    field1 = models.IntegerField()


class DjangoNonrelPaginatorTests(TestCase):
    def setUp(self):
        for i in xrange(12):
            DjangoNonrelPaginationModel.objects.create(field1=i)

    def test_basic_usage(self):
        paginator = DjangoNonrelPaginator(DjangoNonrelPaginationModel.objects.all().order_by("field1"), 5)

        page1 = paginator.page(1)
        self.assertEqual(5, len(page1.object_list))
        self.assertEqual(0, page1.object_list[0].field1)
        self.assertTrue(page1.has_next())
        self.assertFalse(page1.has_previous())
        self.assertEqual([1, 2], page1.available_pages())

        page2 = paginator.page(2)
        self.assertEqual(5, len(page2.object_list))
        self.assertEqual(5, page2.object_list[0].field1)
        self.assertTrue(page2.has_next())
        self.assertTrue(page2.has_previous())
        self.assertEqual([1, 2, 3], page2.available_pages())

        page3 = paginator.page(3)
        self.assertEqual(2, len(page3.object_list))
        self.assertEqual(10, page3.object_list[0].field1)
        self.assertFalse(page3.has_next())
        self.assertTrue(page3.has_previous())
        self.assertEqual([2, 3], page3.available_pages())

        self.assertRaises(EmptyPage, paginator.page, 4)

    def test_cursor_caching(self):
        paginator = DjangoNonrelPaginator(DjangoNonrelPaginationModel.objects.all().order_by("field1"), 5, batch_size=2)

        paginator.page(3)

        self.assertFalse(paginator.has_cursor_for_page(2))
        self.assertFalse(paginator.has_cursor_for_page(3))
        self.assertTrue(paginator.has_cursor_for_page(5))

        paginator.page(1)
        self.assertFalse(paginator.has_cursor_for_page(2))
        self.assertTrue(paginator.has_cursor_for_page(3))
        self.assertTrue(paginator.has_cursor_for_page(5))

        with mock.patch("potatopage.paginator.DjangoNonrelPaginator._process_batch_hook") as mock_obj:
            #Should now use the cached cursor
            page3 = paginator.page(3)
            #Should have been called with a cursor as the 3rd argument
            self.assertTrue(mock_obj.call_args[0][2])

        self.assertEqual(2, len(page3.object_list))
        self.assertEqual(10, page3.object_list[0].field1)

    def test_in_query(self):
        paginator = DjangoNonrelPaginator(DjangoNonrelPaginationModel.objects.filter(field1__in=xrange(12)).all().order_by("field1"), 5)

        page1 = paginator.page(1)
        self.assertEqual(5, len(page1.object_list))
        self.assertEqual(0, page1.object_list[0].field1)
        self.assertTrue(page1.has_next())
        self.assertFalse(page1.has_previous())
        self.assertEqual([1, 2], page1.available_pages())

        page2 = paginator.page(2)
        self.assertEqual(5, len(page2.object_list))
        self.assertEqual(5, page2.object_list[0].field1)
        self.assertTrue(page2.has_next())
        self.assertTrue(page2.has_previous())
        self.assertEqual([1, 2, 3], page2.available_pages())

        page3 = paginator.page(3)
        self.assertEqual(2, len(page3.object_list))
        self.assertEqual(10, page3.object_list[0].field1)
        self.assertFalse(page3.has_next())
        self.assertTrue(page3.has_previous())
        self.assertEqual([2, 3], page3.available_pages())

        self.assertRaises(EmptyPage, paginator.page, 4)

    def test_total_items_count(self):
        """ Test total items count 
            We don't know the real count until we reach the last page 
            and because of that we cannt say what's the total number of items straight away.
            The _get_known_items_count should always return max known number of items (estimated or exact one).
        """

        per_page = 5
        paginator = DjangoNonrelPaginator(DjangoNonrelPaginationModel.objects.all().order_by("field1"), per_page)

        # get the first page
        page = paginator.page(1)
        self.assertEqual(page.paginator._get_known_items_count(), per_page)  # estimated number

        # go to the next (but not last) page
        page = paginator.page(page.next_page_number())
        self.assertEqual(page.paginator._get_known_items_count(), 2 * per_page)  # estimated number

        # go back to the first page
        page = paginator.page(page.previous_page_number())
        self.assertEqual(page.paginator._get_known_items_count(), 2 * per_page)  # estimated number (from cache)

        # go to the next (but not last) page
        page = paginator.page(page.next_page_number())
        self.assertEqual(page.paginator._get_known_items_count(), 2 * per_page)  # estimated number (from cache)

        # go to the last page
        page = paginator.page(page.next_page_number())
        self.assertEqual(page.paginator._get_known_items_count(), 2 * per_page + 2)  # exact number

        # go back to the previous page
        page = paginator.page(page.previous_page_number())
        self.assertEqual(page.paginator._get_known_items_count(), 2 * per_page + 2)  # exact number (from cache)


class GaeNdbPaginationModel(ndb.Model):
    field1 = ndb.IntegerProperty()


class GaeNdbPaginatorTests(TestCase):
    def setUp(self):
        for i in xrange(12):
            pm = GaeNdbPaginationModel(field1=i)
            pm.put()

    def test_basic_usage(self):
        paginator = GaeNdbPaginator(GaeNdbPaginationModel.query().order(GaeNdbPaginationModel.field1), 5)

        page1 = paginator.page(1)
        self.assertEqual(5, len(page1.object_list))
        self.assertEqual(0, page1.object_list[0].field1)
        self.assertTrue(page1.has_next())
        self.assertFalse(page1.has_previous())
        self.assertEqual([1, 2], page1.available_pages())

        page2 = paginator.page(2)
        self.assertEqual(5, len(page2.object_list))
        self.assertEqual(5, page2.object_list[0].field1)
        self.assertTrue(page2.has_next())
        self.assertTrue(page2.has_previous())
        self.assertEqual([1, 2, 3], page2.available_pages())

        page3 = paginator.page(3)
        self.assertEqual(2, len(page3.object_list))
        self.assertEqual(10, page3.object_list[0].field1)
        self.assertFalse(page3.has_next())
        self.assertTrue(page3.has_previous())
        self.assertEqual([2, 3], page3.available_pages())

        self.assertRaises(EmptyPage, paginator.page, 4)

    def test_cursor_caching(self):
        paginator = GaeNdbPaginator(GaeNdbPaginationModel.query().order(GaeNdbPaginationModel.field1), 5, batch_size=2)

        paginator.page(3)

        self.assertFalse(paginator.has_cursor_for_page(2))
        self.assertFalse(paginator.has_cursor_for_page(3))
        self.assertTrue(paginator.has_cursor_for_page(5))

        paginator.page(1)
        self.assertFalse(paginator.has_cursor_for_page(2))
        self.assertTrue(paginator.has_cursor_for_page(3))
        self.assertTrue(paginator.has_cursor_for_page(5))

        with mock.patch("potatopage.paginator.GaeNdbPaginator._process_batch_hook") as mock_obj:
            #Should now use the cached cursor
            page3 = paginator.page(3)
            #Should have been called with a cursor as the 3rd argument
            self.assertTrue(mock_obj.call_args[0][2])

        self.assertEqual(2, len(page3.object_list))
        self.assertEqual(10, page3.object_list[0].field1)
