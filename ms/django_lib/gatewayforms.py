from django_lib import override_forms
from django import forms


class CreateGateway(override_forms.MyForm):

	g_name = forms.CharField(label="Gateway name",
							initial="My Gateway",
							max_length=499,
							help_text="Your gateway's name cannot be changed later.")

	g_password = forms.CharField(label="Gateway password",
                              	max_length=499,
                               	widget=forms.PasswordInput)

	host = forms.CharField(label="Host name",
							max_length=499,)

	port = forms.IntegerField(label="Port number",
								max_value=65535)

	volume_name = forms.CharField(label="Volume name",
                           max_length=499)

class DeleteGateway(override_forms.MyForm):
    
    confirm_delete = forms.BooleanField(required=True,
                                        label="Yes, I understand that this action is permament and my gateway will be gone.")

    g_password = forms.CharField(label="Gateway password",
                               max_length=20,
                               widget=forms.PasswordInput,
                               help_text="You must also own this gateway to delete it.")

class CreateUG(CreateGateway):

    read_write = forms.BooleanField(required=False,
                                    label="UG can write to other gateways.")

# JSON config et al to come for these gateways.
class CreateAG(CreateGateway):
    pass

class CreateRG(CreateGateway):
    pass