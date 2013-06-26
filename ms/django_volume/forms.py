from django_lib import override_forms
from django import forms
from django_lib.override_forms import ReadOnlyWidget

class DeleteVolume(override_forms.MyForm):
    
    confirm_delete = forms.BooleanField(required=True,
                                        label="Yes, I understand that this action is permament and my files will be lost.")

    password = forms.CharField(label="Volume password",
                               max_length=20,
                               help_text="20 characters maximum",
                               widget=forms.PasswordInput)

class Permissions(override_forms.MyForm):
    
    user = forms.EmailField(label="User email",
                            widget=ReadOnlyWidget(),
                            required=False)

    read = forms.BooleanField(label="Read",
                              required=False)
    
    write = forms.BooleanField(label="Write",
                              required=False)


class AddPermissions(override_forms.MyForm):
    
    user = forms.EmailField(label="User email")

    read = forms.BooleanField(label="Read",
                              required=False)
    
    write = forms.BooleanField(label="Write",
                              required=False)
    
class Password(override_forms.MyForm):

    password = forms.CharField(label="Volume password",
                               max_length=20,
                               help_text="20 characters maximum",
                               widget=forms.PasswordInput)