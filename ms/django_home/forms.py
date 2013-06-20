from django import forms

class CreateVolume(forms.Form):
    name = forms.CharField(label="Volume name", initial="My Volume", max_length=20, help_text="20 characters maximum")
    blocksize = forms.IntegerField(label="Desired size of data blocks", initial="TBD Default")
    description = forms.CharField(widget=forms.Textarea, label="Volume description", initial="This is my volume which I'm using to store things for XYZ", max_length=500, help_text="500 characters maximum")
    password = forms.CharField(label="Volume password", max_length=20, help_text="20 characters maximum", widget=forms.PasswordInput)
