from django_lib import override_forms
from django import forms


class Password(override_forms.MyForm):

    password = forms.CharField(label="Password",
                               max_length=20,
                               widget=forms.PasswordInput)

class ChangePassword(override_forms.MyForm):

    oldpassword = forms.CharField(label="Old password",
                               max_length=20,
                               widget=forms.PasswordInput)

    newpassword_1 = forms.CharField(label="New password",
                               max_length=20,
                               widget=forms.PasswordInput)

    newpassword_2 = forms.CharField(label="Re-enter new password",
                               max_length=20,
                               widget=forms.PasswordInput)