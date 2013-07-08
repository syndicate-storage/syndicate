from django_lib import override_forms
from django import forms


class Password(override_forms.MyForm):

    password = forms.CharField(label="Password",
                               max_length=20,
                               widget=forms.PasswordInput)