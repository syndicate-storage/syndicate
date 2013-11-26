from django.db.models.sql.where import WhereNode


def supports_cursor(queryset):
    #First, see if we are using one of Django's built-in connections
    #if we are then, return False
    from django.db import router
    db = router.db_for_read(queryset.query.model)
    compiler = queryset.query.get_compiler(using=db)
    if "django.db." in str(compiler.__class__):
        return False

    def isnt_in_or_exclude_query(queryset):
        lookup = 'in'

        def traverse_where_tree(nodes):
            for n in nodes:
                if not isinstance(n, WhereNode):
                    # This is a leaf node, so it contains the actual query specs
                    if lookup in n:
                        return True
                elif n.negated:
                    return True
                else:
                    in_lookups = traverse_where_tree(n.children)
                    if in_lookups is not None:
                        return in_lookups
        where = queryset.query.where
        if where.negated:
            return False
        return not traverse_where_tree(where.children)

    # It still might not support cursors, so we
    # check if the query doesn't have exclude filters or __in lookups
    return isnt_in_or_exclude_query(queryset)
