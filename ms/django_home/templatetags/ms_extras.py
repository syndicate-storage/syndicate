'''
John Whelchel
Summer 2013

Custom django template tags for metadata website.
'''
from django import template
from django.template.defaultfilters import stringfilter

register = template.Library()

@register.filter
@stringfilter
def replace(value, arg):
	'''
	Django template filter. Takes an argument in the format 'sub1||sub2", and replaces
	all instances of sub1 in value with sub2. Value must be a string type.
	'''
	args = arg.split("||")
	try:
		return value.replace(args[0], args[1])
	except:
		return value