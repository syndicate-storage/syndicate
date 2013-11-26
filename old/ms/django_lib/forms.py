'''

John Whelchel
Summer 2013

Library of forms common to multiple django apps (e.g. volumes and gateways).

Also contains common constants used in different forms.

'''


from django import forms
import potatopage.paginator as potatopaginator

MSPaginator = potatopaginator

LONGEST_CHAR_FIELD = 499
LONGEST_PASS_FIELD = 256
LONGEST_JSON_FIELD = 1000000
LONGEST_DESC = 2000

# http://stackoverflow.com/questions/324477/in-a-django-form-how-to-make-a-field-readonly-or-disabled-so-that-it-cannot-b
class ReadOnlyWidget(forms.widgets.Widget):
    '''
    Allows read only widgets. Useful for volume permission forms.
    '''
    def render(self, _, value, attrs=None):
        return value


class Password(forms.Form):

    password = forms.CharField(label="Password",
                               max_length=LONGEST_PASS_FIELD,
                               widget=forms.PasswordInput)


class ChangePassword(forms.Form):

    oldpassword = forms.CharField(label="Old password",
                               max_length=LONGEST_PASS_FIELD,
                               widget=forms.PasswordInput)

    newpassword_1 = forms.CharField(label="New password",
                               max_length=LONGEST_PASS_FIELD,
                               widget=forms.PasswordInput)

    newpassword_2 = forms.CharField(label="Re-enter new password",
                               max_length=LONGEST_PASS_FIELD,
                               widget=forms.PasswordInput)
    