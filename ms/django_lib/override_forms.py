from django import forms
from django.forms import widgets
#from django.forms.fields import BooleanField, EmailField

# http://stackoverflow.com/questions/324477/in-a-django-form-how-to-make-a-field-readonly-or-disabled-so-that-it-cannot-b
class ReadOnlyWidget(widgets.Widget):
    '''Some of these values are read only - just a bit of text...'''
    def render(self, _, value, attrs=None):
        return value

# No error row
class MyForm(forms.Form):

    def as_p(self):
        "Returns this form rendered as HTML <p>s."
        return self._html_output(
            normal_row = '<p%(html_class_attr)s>%(label)s %(field)s%(help_text)s</p>',
            error_row = '',
            row_ender = '</p>',
            help_text_html = ' <span class="helptext">%s</span>',
            errors_on_separate_row = True)