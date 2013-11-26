from django import template


register = template.Library()

@register.simple_tag(takes_context=True)
def add_to_query_string(context, key, value):
    """ Adds a query to the query string if it exists, otherwise it creates a new one """
    query_string = context['request'].GET.copy()
    query_string[key] = value
    return query_string.urlencode()


@register.simple_tag(takes_context=True)
def paginator_querystring(context, page_number, page_name):
    """ Formats the querystring for the given page number, but keeps the rest of the querystring in tact.
        The page_name argument handles multiple paginators in the same template. The default one is 'page'.
    """
    return add_to_query_string(context, 'page_name', page_number)


@register.simple_tag
def paginator_object_count(page):
    """ Calculate approximate (quick) or the exact (if the last page was reached) count of how many objects are in
        full object_list for pagination count """
    if page.paginator.__class__.__name__ == "DjangoNonrelPaginator" and \
            page.__class__.__name__ == "UnifiedPage":
        total_item_count = page.paginator._get_final_item()

        if not total_item_count:
            more_than_string = 'more than'
            return "%s %d" % (more_than_string,
                page.paginator._get_known_items_count())

        return total_item_count

    # Normal Django Paginator.
    return page.count()
