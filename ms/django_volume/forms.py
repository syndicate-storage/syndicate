from django_lib import override_forms
from django_lib.forms import LONGEST_CHAR_FIELD, LONGEST_PASS_FIELD, LONGEST_DESC
from django import forms
from django_lib.override_forms import ReadOnlyWidget

BLOCKSIZE_MULTIPLIER = 1024 # Kilobytes

BLOCKSIZE_CHOICES = (
    (10*BLOCKSIZE_MULTIPLIER, "10 kB"),
    (20*BLOCKSIZE_MULTIPLIER, 20),
    (40*BLOCKSIZE_MULTIPLIER, 40),
    (80*BLOCKSIZE_MULTIPLIER, 80),
    (160*BLOCKSIZE_MULTIPLIER, 160),
    (320*BLOCKSIZE_MULTIPLIER, 320),
    (640*BLOCKSIZE_MULTIPLIER, 640),
    (1024*BLOCKSIZE_MULTIPLIER,"1 MB"),
)

class CreateVolume(override_forms.MyForm):
 
    name = forms.CharField(label="Volume name",
                           initial="My Volume",
                           max_length=LONGEST_CHAR_FIELD,
                           help_text="Your volume's name cannot be changed later.")

    private = forms.BooleanField(label="Private",
                                  initial=False,
                                  required=False)

    blocksize = forms.ChoiceField(label="Desired size of data blocks",
                                   choices=BLOCKSIZE_CHOICES,
                                   help_text="in kilobytes")
    
    description = forms.CharField(widget=forms.Textarea,
                                  label="Volume description",
                                  initial="This is my new amazing volume.",
                                  max_length=LONGEST_DESC,
                                  help_text=str(LONGEST_DESC) + " characters maximum")
    
    password = forms.CharField(label="Volume password",
                               max_length=LONGEST_PASS_FIELD,
                               widget=forms.PasswordInput)


class ChangeVolumeD(override_forms.MyForm):

    description = forms.CharField(widget=forms.Textarea,
                                  required=False,
                                  label="",
                                  initial="This is my new amazing volume.",
                                  max_length=LONGEST_DESC,
                                  help_text=str(LONGEST_DESC) + " characters maximum")


class DeleteVolume(override_forms.MyForm):
    
    confirm_delete = forms.BooleanField(required=True,
                                        label="Yes, I understand that this action is permament and my files will be lost.")

    password = forms.CharField(label="Volume password",
                               max_length=LONGEST_PASS_FIELD,
                               widget=forms.PasswordInput)

class Gateway(override_forms.MyForm):

    g_name = forms.CharField(label="Gateway name",
                             widget=ReadOnlyWidget(),
                             required=False,
                             max_length=LONGEST_CHAR_FIELD)

    remove = forms.BooleanField(label="Remove",
                                required=False)
        

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