# What is potatopage?

Potatopage contains a paginator class called UnifiedPaginator. This is an attempt
to unify various systems of using cursors to provide pagination on AppEngine.

The idea is that the paginator takes a batch_size argument. This controls the pages
for which a cursor will be stored. For example, a batch_size of 2 would mean that
a cursor would be cached on page 0, page 2, page 4 etc. Accessing page 3 would use
the cursor from page 2, read the entire batch between page 2 and 4, and then offset
into the results.

# How can I use it?

That's pretty easy, depending on the datastore you'd like to use, pick the right paginator and just use it as you'd use a [Django paginator](https://docs.djangoproject.com/en/dev/topics/pagination/). Here the two available paginators with a quick example:

1. Django-nonrel Paginator

		from potatopage.paginator import DjangoNonrelPaginator
		# Import your Django-nonrel model.
		from your_app.models import MyModel
		
		queryset = MyModel.objects.filter(…).order_by(…)
		paginator = DjangoNonrelPaginator(queryset, per_page=10, batch_size=2)
		page1 = paginator.page(1)

2. App Engine NDB Paginator

		from potatopage.paginator import GaeNdbPaginator
		# Import your App Engine NDB model
		from my_models import MyModel 
		
		query = MyModel.query(…).order(...)
		paginator = GaeNdbPaginator(query, per_page=10, batch_size=2)
		page1 = paginator.page(1)
		
The `page1` you get in return is an instance of `UnifiedPage` and can be used like a Django paginator page. 

### Note:

The only bit to keep in mind is that all these paginators won't query all objects in one go, it does the queries limited to the size of the batch you specify with `batch_size`. I.e. calls like e.g. `paginator.num_pages` aren't possible. To make work with this `UnifiedPaginator` subclasses a bit easier, other properties were added to the returned page:

* `page.available_pages()`: returns a list of page numbers that have already been queried by the paginator. 
* `page.final_page_visible()`: checks if the list of page numbers returned by `page.available_pages()` contains the final page or not and returns the result as a boolean.

Also, remember to add potatopage app to you INSTALLED_APPS if you want to use potatopage temlpate tags in your templates
