from django_lib import override_forms
from django import forms

LONGEST_CHAR_FIELD = 499
LONGEST_PASS_FIELD = 20
LONGEST_JSON_FIELD = 5000
LONGEST_DESC = 2000

class Password(override_forms.MyForm):

    password = forms.CharField(label="Password",
                               max_length=LONGEST_PASS_FIELD,
                               widget=forms.PasswordInput)

class ChangePassword(override_forms.MyForm):

    oldpassword = forms.CharField(label="Old password",
                               max_length=LONGEST_PASS_FIELD,
                               widget=forms.PasswordInput)

    newpassword_1 = forms.CharField(label="New password",
                               max_length=LONGEST_PASS_FIELD,
                               widget=forms.PasswordInput)

    newpassword_2 = forms.CharField(label="Re-enter new password",
                               max_length=LONGEST_PASS_FIELD,
                               widget=forms.PasswordInput)