from django_lib import override_forms
from django import forms

class CreateVolume(override_forms.MyForm):
    name = forms.CharField(label="Volume name", initial="My_Volume", max_length=20, help_text="20 characters maximum, no spaces.")
    blocksize = forms.IntegerField(label="Desired size of data blocks", initial="61440", help_text="Bytes. Don't use less than a few KB.", min_value=1, max_value=100000)
    description = forms.CharField(widget=forms.Textarea, label="Volume description", initial="This is my volume which I'm using to store amazing things and wow the world.", max_length=500, help_text="500 characters maximum")
    password = forms.CharField(label="Volume password", max_length=20, help_text="20 characters maximum", widget=forms.PasswordInput)
