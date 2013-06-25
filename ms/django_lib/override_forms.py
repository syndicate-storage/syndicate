from django import forms
from django.forms import widgets
#from django.forms.fields import BooleanField, EmailField


'''
class DisabledBooleanField(BooleanField):
    def widget_attrs(self, widget):
        attrs = super(BooleanField, self).widget_attrs(widget)
        attrs['disabled'] = ""
        return attrs

class DisabledEmailField(EmailField):
    def widget_attrs(self, widget):
        attrs = super(EmailField, self).widget_attrs(widget)
        attrs['disabled'] = ""
        return attrs
'''

# http://stackoverflow.com/questions/324477/in-a-django-form-how-to-make-a-field-readonly-or-disabled-so-that-it-cannot-b
class ReadOnlyWidget(widgets.Widget):
    '''Some of these values are read only - just a bit of text...'''
    def render(self, _, value, attrs=None):
        return value


class MyForm(forms.Form):

    def as_p(self):
        "Returns this form rendered as HTML <p>s."
        return self._html_output(
            normal_row = '<p%(html_class_attr)s>%(label)s %(field)s%(help_text)s</p>',
            error_row = '',
            row_ender = '</p>',
            help_text_html = ' <span class="helptext">%s</span>',
            errors_on_separate_row = True)


'''
    def _html_output(self, normal_row, error_row, row_ender, help_text_html, errors_on_separate_row):
        "Helper function for outputting HTML. Used by as_table(), as_ul(), as_p(v)."
        top_errors = self.non_field_errors() # Errors that should be displayed above all fields.
        output, hidden_fields = [], []

        for name, field in self.fields.items():
            html_class_attr = ''
            bf = self[name]
            # Escape and cache in local variable.
            bf_errors = self.error_class([conditional_escape(error) for error in bf.errors])
            if bf.is_hidden:
                if bf_errors:
                    top_errors.extend(
                        [_('(Hidden field %(name)s) %(error)s') % {'name': name, 'error': force_text(e)}
                         for e in bf_errors])
                hidden_fields.append(six.text_type(bf))
            else:
                # Create a 'class="..."' atribute if the row should have any
                # CSS classes applied.
                css_classes = bf.css_classes()
                if css_classes:
                    html_class_attr = ' class="%s"' % css_classes

#                if errors_on_separate_row and bf_errors:
#                    output.append(error_row % force_text(bf_errors))

                if bf.label:
                    label = conditional_escape(force_text(bf.label))
                    label = bf.label_tag(label) or ''
                else:
                    label = ''

                if field.help_text:
                    help_text = help_text_html % force_text(field.help_text)
                else:
                    help_text = ''

                output.append(normal_row % {
                    'errors': force_text(bf_errors),
                    'label': force_text(label),
                    'field': six.text_type(bf),
                    'help_text': help_text,
                    'html_class_attr': html_class_attr
                })

#        if top_errors:
#            output.insert(0, error_row % force_text(top_errors))

        if hidden_fields: # Insert any hidden fields in the last row.
            str_hidden = ''.join(hidden_fields)
            if output:
                last_row = output[-1]
                # Chop off the trailing row_ender (e.g. '</td></tr>') and
                # insert the hidden fields.
                if not last_row.endswith(row_ender):
                    # This can happen in the as_p() case (and possibly others
                    # that users write): if there are only top errors, we may
                    # not be able to conscript the last row for our purposes,
                    # so insert a new, empty row.
                    last_row = (normal_row % {'errors': '', 'label': '',
                                              'field': '', 'help_text':'',
                                              'html_class_attr': html_class_attr})
                    output.append(last_row)
                output[-1] = last_row[:-len(row_ender)] + str_hidden + row_ender
            else:
                # If there aren't any rows in the output, just append the
                # hidden fields.
                output.append(str_hidden)
        return mark_safe('\n'.join(output))
'''
